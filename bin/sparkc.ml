(* sparkc: run SQL against a Spark Connect server from the terminal.

   sparkc [-r sc://host:port] [-e "SELECT ..."] [-f file.sql] [-n ROWS] [--csv|--json|--table]
   With no -e/-f, reads statements from stdin (one per line, or ';'-terminated). *)

open Lwt.Infix
open Spark_connect

let remote = ref (try Sys.getenv "SPARK_REMOTE" with Not_found -> "sc://localhost:15002")
let queries = ref []
let files = ref []
let rows = ref 20
let format = ref `Table
let truncate = ref 0
let timing = ref false

let usage =
  "sparkc [options] [-e SQL]...\nRun SQL on a Spark Connect server (default: $SPARK_REMOTE or sc://localhost:15002)."

let spec =
  [
    ("-r", Arg.Set_string remote, "URL  Spark Connect URL, e.g. sc://host:15002/;token=...");
    ("-e", Arg.String (fun q -> queries := q :: !queries), "SQL  statement to run (repeatable)");
    ("-f", Arg.String (fun f -> files := f :: !files), "FILE file of ';'-separated statements");
    ("-n", Arg.Set_int rows, "N    rows to show (default 20)");
    ("--truncate", Arg.Set_int truncate, "N  truncate cells to N chars (0 = never)");
    ("--csv", Arg.Unit (fun () -> format := `Csv), " output CSV instead of a table");
    ("--json", Arg.Unit (fun () -> format := `Json), " output JSON lines");
    ("--table", Arg.Unit (fun () -> format := `Table), " output Spark's show() table (default)");
    ("-t", Arg.Set timing, " print elapsed time per statement");
  ]

let json_escape s =
  let b = Buffer.create (String.length s + 2) in
  Buffer.add_char b '"';
  String.iter
    (function
      | '"' -> Buffer.add_string b "\\\""
      | '\\' -> Buffer.add_string b "\\\\"
      | '\n' -> Buffer.add_string b "\\n"
      | '\r' -> Buffer.add_string b "\\r"
      | '\t' -> Buffer.add_string b "\\t"
      | c when Char.code c < 0x20 -> Buffer.add_string b (Printf.sprintf "\\u%04x" (Char.code c))
      | c -> Buffer.add_char b c)
    s;
  Buffer.add_char b '"';
  Buffer.contents b

let rec json_of_value (v : Value.t) =
  match v with
  | Null -> "null"
  | Bool b -> string_of_bool b
  | Int i -> string_of_int i
  | Int64 i -> Int64.to_string i
  | Float f ->
      if Float.is_nan f || (Float.is_integer f = false && Float.abs f = infinity) then "null"
      else Printf.sprintf "%.17g" f
  | List vs -> "[" ^ String.concat "," (Array.to_list (Array.map json_of_value vs)) ^ "]"
  | Struct fs ->
      "{" ^ String.concat "," (Array.to_list (Array.map (fun (k, v) -> json_escape k ^ ":" ^ json_of_value v) fs)) ^ "}"
  | Map kvs ->
      "{"
      ^ String.concat ","
          (Array.to_list (Array.map (fun (k, v) -> json_escape (Value.to_string k) ^ ":" ^ json_of_value v) kvs))
      ^ "}"
  | v -> json_escape (Value.to_string v)

let csv_cell (v : Value.t) =
  match v with
  | Null -> ""
  | v ->
      let s = Value.to_string v in
      if String.exists (fun c -> c = ',' || c = '"' || c = '\n') s then
        "\"" ^ String.concat "\"\"" (String.split_on_char '"' s) ^ "\""
      else s

let run_statement spark stmt =
  let t0 = Unix.gettimeofday () in
  Session.sql spark stmt >>= fun df ->
  (match !format with
    | `Table -> Dataframe.show ~n:!rows ~truncate:!truncate df
    | `Csv ->
        Dataframe.collect (Dataframe.limit df !rows) >|= fun rs ->
        (match rs with r :: _ -> print_endline (String.concat "," (Array.to_list (Row.names r))) | [] -> ());
        List.iter (fun r -> print_endline (String.concat "," (List.map csv_cell (Row.to_list r)))) rs
    | `Json ->
        Dataframe.collect (Dataframe.limit df !rows) >|= fun rs ->
        List.iter
          (fun r ->
            print_endline
              ("{"
              ^ String.concat "," (List.map (fun (k, v) -> json_escape k ^ ":" ^ json_of_value v) (Row.to_alist r))
              ^ "}"))
          rs)
  >|= fun () -> if !timing then Printf.printf "-- %.3fs\n%!" (Unix.gettimeofday () -. t0)

let split_statements text =
  String.split_on_char ';' text |> List.map String.trim
  |> List.filter (fun s -> s <> "" && not (String.length s >= 2 && String.sub s 0 2 = "--"))

let read_file f =
  let ic = open_in_bin f in
  let s = really_input_string ic (in_channel_length ic) in
  close_in ic;
  s

let main () =
  Arg.parse spec (fun q -> queries := q :: !queries) usage;
  let stmts = List.rev !queries @ List.concat_map (fun f -> split_statements (read_file f)) (List.rev !files) in
  Session.connect !remote >>= fun spark ->
  let safe stmt =
    Lwt.catch
      (fun () -> run_statement spark stmt)
      (function
        | Spark_error e ->
            prerr_endline (Error.to_string e);
            Lwt.return_unit
        | Grpc_client.Transport_error m ->
            prerr_endline ("transport error: " ^ m);
            exit 2
        | e -> Lwt.fail e)
  in
  (if stmts <> [] then Lwt_list.iter_s safe stmts
   else begin
     let interactive = Unix.isatty Unix.stdin in
     if interactive then
       Session.version spark >|= fun v ->
       Printf.printf "Spark %s at %s. End statements with ';'. Ctrl-D exits.\n%!" v !remote
     else
       Lwt.return_unit >>= fun () ->
       let buf = Buffer.create 256 in
       let rec loop () =
         if interactive then (
           print_string (if Buffer.length buf = 0 then "spark> " else "    -> ");
           flush stdout);
         Lwt_io.read_line_opt Lwt_io.stdin >>= function
         | None ->
             if String.trim (Buffer.contents buf) <> "" then safe (String.trim (Buffer.contents buf))
             else Lwt.return_unit
         | Some line ->
             Buffer.add_string buf line;
             Buffer.add_char buf '\n';
             let text = Buffer.contents buf in
             if String.contains text ';' then begin
               let parts = String.split_on_char ';' text in
               let complete = List.filteri (fun i _ -> i < List.length parts - 1) parts in
               let rest = List.nth parts (List.length parts - 1) in
               Buffer.clear buf;
               Buffer.add_string buf rest;
               Lwt_list.iter_s (fun s -> if String.trim s = "" then Lwt.return_unit else safe (String.trim s)) complete
               >>= loop
             end
             else loop ()
       in
       loop ()
   end)
  >>= fun () -> Session.close spark

let () = Lwt_main.run (main ())
