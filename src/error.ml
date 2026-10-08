(* Errors reported by the Spark Connect server, decoded from gRPC status
   details ([google.rpc.Status] carrying a [google.rpc.ErrorInfo]). *)

module P = Spark_connect_proto

type t = {
  code : int;  (** gRPC status code *)
  message : string;
  error_class : string option;  (** Spark error condition, e.g. ["DIVIDE_BY_ZERO"], parsed from the message *)
  exception_classes : string list;  (** JVM exception class hierarchy, most specific first *)
  sql_state : string option;
  error_id : string option;  (** id usable with [FetchErrorDetails] *)
  metadata : (string * string) list;  (** raw [ErrorInfo] metadata *)
}

exception Spark_error of t

let grpc_code_name = function
  | 0 -> "OK"
  | 1 -> "CANCELLED"
  | 2 -> "UNKNOWN"
  | 3 -> "INVALID_ARGUMENT"
  | 4 -> "DEADLINE_EXCEEDED"
  | 5 -> "NOT_FOUND"
  | 6 -> "ALREADY_EXISTS"
  | 7 -> "PERMISSION_DENIED"
  | 8 -> "RESOURCE_EXHAUSTED"
  | 9 -> "FAILED_PRECONDITION"
  | 10 -> "ABORTED"
  | 11 -> "OUT_OF_RANGE"
  | 12 -> "UNIMPLEMENTED"
  | 13 -> "INTERNAL"
  | 14 -> "UNAVAILABLE"
  | 15 -> "DATA_LOSS"
  | 16 -> "UNAUTHENTICATED"
  | n -> Printf.sprintf "CODE_%d" n

(* "classes" arrives as a JSON array of strings; a tolerant split is enough. *)
let parse_classes s =
  let s = String.trim s in
  let s = if String.length s >= 2 && s.[0] = '[' then String.sub s 1 (String.length s - 2) else s in
  String.split_on_char ',' s
  |> List.map (fun c ->
      String.trim c |> fun c -> if String.length c >= 2 && c.[0] = '"' then String.sub c 1 (String.length c - 2) else c)
  |> List.filter (fun c -> c <> "")

let ends_with ~suffix s =
  let ls = String.length s and lf = String.length suffix in
  ls >= lf && String.sub s (ls - lf) lf = suffix

(* Spark messages start with "[ERROR_CLASS] ..." *)
let parse_error_class msg =
  if String.length msg > 2 && msg.[0] = '[' then
    match String.index_opt msg ']' with
    | Some i when i > 1 ->
        let c = String.sub msg 1 (i - 1) in
        if String.for_all (fun ch -> (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch = '_' || ch = '.') c
        then Some c
        else None
    | _ -> None
  else None

let of_grpc (st : Grpc_client.status) =
  let base =
    {
      code = st.code;
      message = st.message;
      error_class = parse_error_class st.message;
      exception_classes = [];
      sql_state = None;
      error_id = None;
      metadata = [];
    }
  in
  match st.details with
  | None -> base
  | Some raw -> (
      try
        let status = P.Status.decode_pb_status (Pbrt.Decoder.of_string raw) in
        let info =
          List.find_map
            (fun (a : P.Any.any) ->
              if ends_with ~suffix:"google.rpc.ErrorInfo" a.type_url then
                Some (P.Error_details.decode_pb_error_info (Pbrt.Decoder.of_string (Bytes.to_string a.value)))
              else None)
            status.details
        in
        match info with
        | None -> base
        | Some info ->
            let md = info.metadata in
            {
              base with
              exception_classes = Option.value ~default:[] (Option.map parse_classes (List.assoc_opt "classes" md));
              sql_state = List.assoc_opt "sqlState" md;
              error_id = List.assoc_opt "errorId" md;
              metadata = md;
            }
      with _ -> base)

let to_string e =
  let cls = match e.exception_classes with [] -> "" | c :: _ -> Printf.sprintf " (%s)" c in
  Printf.sprintf "Spark error %s%s: %s" (grpc_code_name e.code) cls e.message

let () =
  Printexc.register_printer (function
    | Spark_error e -> Some (to_string e)
    | Grpc_client.Grpc_error st -> Some (Printf.sprintf "gRPC error %s: %s" (grpc_code_name st.code) st.message)
    | Grpc_client.Transport_error m -> Some ("Spark Connect transport error: " ^ m)
    | _ -> None)

(** Re-raise gRPC failures as [Spark_error]. *)
let wrap (f : unit -> 'a Lwt.t) : 'a Lwt.t =
  Lwt.catch f (function Grpc_client.Grpc_error st -> Lwt.fail (Spark_error (of_grpc st)) | e -> Lwt.fail e)
