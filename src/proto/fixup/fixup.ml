(* Post-process ocaml-protoc output.

   ocaml-protoc 4.1 emits, for a oneof whose first case is a message in the
   same recursive group, a one-line default such as

     let default_foo_kind (): foo_kind = Bar (default_bar ())

   before [default_bar] is defined. This program moves any such line to just
   after the definition it depends on. It is a no-op on files that are already
   well-ordered. Usage: fixup FILE... (rewrites in place). *)

(* ocaml-protoc 4.1 also emits, for a unit-typed oneof case inside the
   standalone encoder of a oneof-only message,

     Pbrt.Encoder.key N Pbrt.Bytes encoder;
     Pbrt.Encoder.empty_nested encoder

   but its encoder writes back-to-front, so the payload must be written before
   the key (as the inlined encoders correctly do). We swap the two lines. *)
let key_line = Str.regexp {|^    Pbrt.Encoder.key [0-9]+ Pbrt.Bytes encoder; *$|}
let empty_line = Str.regexp {|^    Pbrt.Encoder.empty_nested encoder *$|}

let swap_empty_nested (lines : string array) =
  let n = Array.length lines in
  let out = ref [] in
  let i = ref 0 in
  while !i < n do
    if !i + 1 < n && Str.string_match key_line lines.(!i) 0 && Str.string_match empty_line lines.(!i + 1) 0 then begin
      out := String.trim lines.(!i) :: "    Pbrt.Encoder.empty_nested encoder;" :: !out;
      (* re-indent the key line *)
      (match !out with
      | k :: rest -> out := ("    " ^ k) :: rest
      | [] -> ());
      i := !i + 2
    end
    else begin
      out := lines.(!i) :: !out;
      incr i
    end
  done;
  Array.of_list (List.rev !out)

let one_liner =
  Str.regexp
    {|^let \(default_[A-Za-z0-9_']+\) ()\(: [A-Za-z0-9_'.]+\)? = [A-Za-z0-9_'.]+ (\(default_[A-Za-z0-9_']+\) ())$|}

let def_start name = Str.regexp ("^\\(let\\|let rec\\|and\\) " ^ Str.quote name ^ " ")

let read_lines file =
  let ic = open_in_bin file in
  let rec go acc =
    match input_line ic with
    | l -> go (l :: acc)
    | exception End_of_file ->
        close_in ic;
        List.rev acc
  in
  go []

let write_lines file lines =
  let oc = open_out_bin file in
  List.iter
    (fun l ->
      output_string oc l;
      output_char oc '\n')
    lines;
  close_out oc

let find_index p a =
  let n = Array.length a in
  let rec go i = if i >= n then None else if p a.(i) then Some i else go (i + 1) in
  go 0

(* One pass: returns [Some lines] if a move was made, [None] if nothing to do. *)
let pass (lines : string array) =
  let n = Array.length lines in
  let rec scan i =
    if i >= n then None
    else if Str.string_match one_liner lines.(i) 0 then begin
      let dep = Str.matched_group 3 lines.(i) in
      let re = def_start dep in
      match find_index (fun l -> Str.string_match re l 0) lines with
      | Some j when j > i ->
          (* end of dep's block: first blank line at or after j *)
          let rec block_end k = if k >= n || lines.(k) = "" then k else block_end (k + 1) in
          let e = block_end j in
          let moved = lines.(i) in
          let out = ref [] in
          Array.iteri
            (fun k l ->
              if k = i then ()
              else begin
                out := l :: !out;
                if k = e - 1 then out := moved :: "" :: !out
              end)
            lines;
          (* if the dep block ran to EOF, append *)
          if e >= n then out := moved :: !out;
          Some (Array.of_list (List.rev !out))
      | _ -> scan (i + 1)
    end
    else scan (i + 1)
  in
  scan 0

let fix file =
  let rec loop lines budget =
    if budget = 0 then failwith ("fixup: no fixed point for " ^ file)
    else match pass lines with None -> lines | Some lines' -> loop lines' (budget - 1)
  in
  let lines = Array.of_list (read_lines file) in
  let fixed = swap_empty_nested (loop lines 1000) in
  if fixed <> lines then write_lines file (Array.to_list fixed)

let () = Array.iteri (fun i f -> if i > 0 then fix f) Sys.argv
