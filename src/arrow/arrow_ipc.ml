(* Arrow IPC stream reader.

   Decodes the Arrow IPC streaming format (Schema message followed by
   RecordBatch messages) into [Value.t] columns. Covers the types Spark
   produces: primitives, strings, binary, decimals, dates, times, timestamps,
   intervals, lists, structs and maps. Dictionary-encoded columns, unions,
   run-end encoding and compressed bodies are reported as [Unsupported]. *)

exception Unsupported of string
exception Malformed of string

let unsupported fmt = Printf.ksprintf (fun s -> raise (Unsupported s)) fmt
let malformed fmt = Printf.ksprintf (fun s -> raise (Malformed s)) fmt

type time_unit = Value.time_unit = Second | Millisecond | Microsecond | Nanosecond
type date_unit = Day | Milli
type interval_unit = Year_month | Day_time | Month_day_nano

type typ =
  | Null
  | Bool
  | Int of { bits : int; signed : bool }
  | Float of { bits : int }
  | Utf8
  | Large_utf8
  | Binary
  | Large_binary
  | Fixed_size_binary of int
  | Decimal of { precision : int; scale : int; bits : int }
  | Date of date_unit
  | Time of { unit : time_unit; bits : int }
  | Timestamp of { unit : time_unit; tz : string option }
  | Duration of time_unit
  | Interval of interval_unit
  | List
  | Large_list
  | Fixed_size_list of int
  | Struct
  | Map of { keys_sorted : bool }
  | Other of string  (** a type this reader does not decode; name is informational *)

type field = { name : string; nullable : bool; typ : typ; children : field list; metadata : (string * string) list }
type schema = { fields : field list; schema_metadata : (string * string) list }

type batch = { num_rows : int; columns : Value.t array array }
(** A decoded record batch: one [Value.t array] per top-level field. *)

let rec typ_to_string = function
  | Null -> "null"
  | Bool -> "bool"
  | Int { bits; signed } -> Printf.sprintf "%sint%d" (if signed then "" else "u") bits
  | Float { bits } -> Printf.sprintf "float%d" bits
  | Utf8 -> "utf8"
  | Large_utf8 -> "large_utf8"
  | Binary -> "binary"
  | Large_binary -> "large_binary"
  | Fixed_size_binary n -> Printf.sprintf "fixed_size_binary[%d]" n
  | Decimal { precision; scale; bits } -> Printf.sprintf "decimal%d(%d,%d)" bits precision scale
  | Date Day -> "date32"
  | Date Milli -> "date64"
  | Time { unit; bits } -> Printf.sprintf "time%d[%s]" bits (Value.unit_to_string unit)
  | Timestamp { unit; tz } ->
      Printf.sprintf "timestamp[%s%s]" (Value.unit_to_string unit) (match tz with None -> "" | Some z -> ", " ^ z)
  | Duration u -> Printf.sprintf "duration[%s]" (Value.unit_to_string u)
  | Interval Year_month -> "interval[year_month]"
  | Interval Day_time -> "interval[day_time]"
  | Interval Month_day_nano -> "interval[month_day_nano]"
  | List -> "list"
  | Large_list -> "large_list"
  | Fixed_size_list n -> Printf.sprintf "fixed_size_list[%d]" n
  | Struct -> "struct"
  | Map _ -> "map"
  | Other s -> s

and field_to_string f =
  let base = Printf.sprintf "%s: %s%s" f.name (typ_to_string f.typ) (if f.nullable then "" else " not null") in
  match f.children with [] -> base | cs -> base ^ "<" ^ String.concat ", " (List.map field_to_string cs) ^ ">"

let schema_to_string s = String.concat "\n" (List.map field_to_string s.fields)

(* ---- Metadata (FlatBuffers) ---- *)

module Fb = Flatbuf

let time_unit_of_int = function
  | 0 -> Second
  | 1 -> Millisecond
  | 2 -> Microsecond
  | 3 -> Nanosecond
  | n -> malformed "bad time unit %d" n

let key_values t id =
  List.map
    (fun kv -> (Option.value ~default:"" (Fb.get_string kv 0), Option.value ~default:"" (Fb.get_string kv 1)))
    (Fb.get_table_vector t id)

(* Field.type union, by type id (org.apache.arrow.flatbuf.Type). *)
let parse_type type_id (t : Fb.table option) =
  let tbl () = match t with Some t -> t | None -> malformed "missing type table for type id %d" type_id in
  match type_id with
  | 1 -> Null
  | 2 ->
      let t = tbl () in
      Int { bits = Fb.get_i32 t 0; signed = Fb.get_bool t 1 }
  | 3 ->
      let t = tbl () in
      Float
        { bits = (match Fb.get_i16 t 0 with 0 -> 16 | 1 -> 32 | 2 -> 64 | p -> malformed "bad float precision %d" p) }
  | 4 -> Binary
  | 5 -> Utf8
  | 6 -> Bool
  | 7 ->
      let t = tbl () in
      Decimal { precision = Fb.get_i32 t 0; scale = Fb.get_i32 t 1; bits = Fb.get_i32 ~default:128 t 2 }
  | 8 ->
      let t = tbl () in
      Date (match Fb.get_i16 t 0 with 0 -> Day | _ -> Milli)
  | 9 ->
      let t = tbl () in
      Time { unit = time_unit_of_int (Fb.get_i16 ~default:1 t 0); bits = Fb.get_i32 ~default:32 t 1 }
  | 10 ->
      let t = tbl () in
      Timestamp { unit = time_unit_of_int (Fb.get_i16 t 0); tz = Fb.get_string t 1 }
  | 11 ->
      let t = tbl () in
      Interval (match Fb.get_i16 t 0 with 0 -> Year_month | 1 -> Day_time | _ -> Month_day_nano)
  | 12 -> List
  | 13 -> Struct
  | 14 -> Other "union"
  | 15 ->
      let t = tbl () in
      Fixed_size_binary (Fb.get_i32 t 0)
  | 16 ->
      let t = tbl () in
      Fixed_size_list (Fb.get_i32 t 0)
  | 17 ->
      let t = tbl () in
      Map { keys_sorted = Fb.get_bool t 0 }
  | 18 ->
      let t = tbl () in
      Duration (time_unit_of_int (Fb.get_i16 ~default:1 t 0))
  | 19 -> Large_binary
  | 20 -> Large_utf8
  | 21 -> Large_list
  | 22 -> Other "run_end_encoded"
  | 23 -> Other "binary_view"
  | 24 -> Other "utf8_view"
  | 25 -> Other "list_view"
  | 26 -> Other "large_list_view"
  | n -> Other (Printf.sprintf "type#%d" n)

let rec parse_field (t : Fb.table) =
  if Fb.get_table t 4 <> None then
    unsupported "dictionary-encoded field %S" (Option.value ~default:"" (Fb.get_string t 0));
  {
    name = Option.value ~default:"" (Fb.get_string t 0);
    nullable = Fb.get_bool t 1;
    typ = parse_type (Fb.get_u8 t 2) (Fb.get_table t 3);
    children = List.map parse_field (Fb.get_table_vector t 5);
    metadata = key_values t 6;
  }

let parse_schema (t : Fb.table) =
  { fields = List.map parse_field (Fb.get_table_vector t 1); schema_metadata = key_values t 2 }

(* ---- Record batch bodies ---- *)

type node = { length : int; null_count : int }
type buffer = { off : int; len : int }

(* A cursor over the nodes and buffers of one record batch. *)
type cursor = { body : string; body_off : int; mutable nodes : node list; mutable buffers : buffer list }

let next_node c =
  match c.nodes with
  | n :: r ->
      c.nodes <- r;
      n
  | [] -> malformed "record batch has too few field nodes"

let next_buffer c =
  match c.buffers with
  | b :: r ->
      c.buffers <- r;
      b
  | [] -> malformed "record batch has too few buffers"

let check_buf c b need =
  if b.len < need then malformed "buffer too short: have %d, need %d" b.len need;
  if b.off < 0 || c.body_off + b.off + b.len > String.length c.body then malformed "buffer out of body bounds"

(* Validity accessor: [None] means every slot is valid. *)
let validity c (n : node) =
  let b = next_buffer c in
  if n.null_count = 0 || b.len = 0 then fun _ -> true
  else begin
    check_buf c b ((n.length + 7) / 8);
    let base = c.body_off + b.off in
    fun i -> (Char.code (String.unsafe_get c.body (base + (i lsr 3))) lsr (i land 7)) land 1 = 1
  end

let bitmap c (n : node) =
  let b = next_buffer c in
  check_buf c b ((n.length + 7) / 8);
  let base = c.body_off + b.off in
  fun i -> (Char.code (String.unsafe_get c.body (base + (i lsr 3))) lsr (i land 7)) land 1 = 1

let data_buffer c (n : node) ~width =
  let b = next_buffer c in
  check_buf c b (n.length * width);
  c.body_off + b.off

let offsets32 c (n : node) =
  let b = next_buffer c in
  check_buf c b (if n.length = 0 then 0 else 4 * (n.length + 1));
  let base = c.body_off + b.off in
  fun i -> Int32.to_int (String.get_int32_le c.body (base + (4 * i)))

let offsets64 c (n : node) =
  let b = next_buffer c in
  check_buf c b (if n.length = 0 then 0 else 8 * (n.length + 1));
  let base = c.body_off + b.off in
  fun i -> Int64.to_int (String.get_int64_le c.body (base + (8 * i)))

let half_to_float h =
  let s = (h lsr 15) land 1 and e = (h lsr 10) land 0x1f and m = h land 0x3ff in
  let v =
    if e = 0 then ldexp (float_of_int m) (-24)
    else if e = 31 then if m = 0 then infinity else nan
    else ldexp (float_of_int (m lor 0x400)) (e - 25)
  in
  if s = 1 then -.v else v

let rec decode_column c (f : field) : Value.t array =
  let n = next_node c in
  let len = n.length in
  let s = c.body in
  let gen valid read = Array.init len (fun i -> if valid i then read i else Value.Null) in
  match f.typ with
  | Null -> Array.make len Value.Null
  | Bool ->
      let valid = validity c n in
      let bit = bitmap c n in
      gen valid (fun i -> Value.Bool (bit i))
  | Int { bits; signed } ->
      let valid = validity c n in
      let base = data_buffer c n ~width:(bits / 8) in
      let read =
        match (bits, signed) with
        | 8, true -> fun i -> Value.Int (String.get_int8 s (base + i))
        | 8, false -> fun i -> Value.Int (String.get_uint8 s (base + i))
        | 16, true -> fun i -> Value.Int (String.get_int16_le s (base + (2 * i)))
        | 16, false -> fun i -> Value.Int (String.get_uint16_le s (base + (2 * i)))
        | 32, true -> fun i -> Value.Int (Int32.to_int (String.get_int32_le s (base + (4 * i))))
        | 32, false -> fun i -> Value.Int (Int32.to_int (String.get_int32_le s (base + (4 * i))) land 0xFFFF_FFFF)
        | 64, _ -> fun i -> Value.Int64 (String.get_int64_le s (base + (8 * i)))
        | _ -> unsupported "int%d" bits
      in
      gen valid read
  | Float { bits } ->
      let valid = validity c n in
      let base = data_buffer c n ~width:(bits / 8) in
      let read =
        match bits with
        | 16 -> fun i -> Value.Float (half_to_float (String.get_uint16_le s (base + (2 * i))))
        | 32 -> fun i -> Value.Float (Int32.float_of_bits (String.get_int32_le s (base + (4 * i))))
        | 64 -> fun i -> Value.Float (Int64.float_of_bits (String.get_int64_le s (base + (8 * i))))
        | _ -> unsupported "float%d" bits
      in
      gen valid read
  | Utf8 | Binary ->
      let valid = validity c n in
      let off = offsets32 c n in
      let data = next_buffer c in
      let base = c.body_off + data.off in
      let mk = if f.typ = Utf8 then fun x -> Value.String x else fun x -> Value.Binary x in
      gen valid (fun i ->
          let a = off i and b = off (i + 1) in
          if a < 0 || b < a || b > data.len then malformed "bad string offsets";
          mk (String.sub s (base + a) (b - a)))
  | Large_utf8 | Large_binary ->
      let valid = validity c n in
      let off = offsets64 c n in
      let data = next_buffer c in
      let base = c.body_off + data.off in
      let mk = if f.typ = Large_utf8 then fun x -> Value.String x else fun x -> Value.Binary x in
      gen valid (fun i ->
          let a = off i and b = off (i + 1) in
          if a < 0 || b < a || b > data.len then malformed "bad string offsets";
          mk (String.sub s (base + a) (b - a)))
  | Fixed_size_binary w ->
      let valid = validity c n in
      let base = data_buffer c n ~width:w in
      gen valid (fun i -> Value.Binary (String.sub s (base + (w * i)) w))
  | Decimal { scale; bits; _ } ->
      let valid = validity c n in
      let w = bits / 8 in
      let base = data_buffer c n ~width:w in
      gen valid (fun i -> Value.Decimal (Value.decimal_of_le_bytes s ~off:(base + (w * i)) ~len:w ~scale))
  | Date Day ->
      let valid = validity c n in
      let base = data_buffer c n ~width:4 in
      gen valid (fun i -> Value.Date (Int32.to_int (String.get_int32_le s (base + (4 * i)))))
  | Date Milli ->
      let valid = validity c n in
      let base = data_buffer c n ~width:8 in
      gen valid (fun i -> Value.Date (Int64.to_int (Value.fdiv (String.get_int64_le s (base + (8 * i))) 86_400_000L)))
  | Time { unit; bits } ->
      let valid = validity c n in
      let base = data_buffer c n ~width:(bits / 8) in
      gen valid (fun i ->
          let v =
            if bits = 32 then Int64.of_int32 (String.get_int32_le s (base + (4 * i)))
            else String.get_int64_le s (base + (8 * i))
          in
          Value.Time (v, unit))
  | Timestamp { unit; tz } ->
      let valid = validity c n in
      let base = data_buffer c n ~width:8 in
      gen valid (fun i -> Value.Timestamp (String.get_int64_le s (base + (8 * i)), unit, tz))
  | Duration unit ->
      let valid = validity c n in
      let base = data_buffer c n ~width:8 in
      gen valid (fun i -> Value.Duration (String.get_int64_le s (base + (8 * i)), unit))
  | Interval Year_month ->
      let valid = validity c n in
      let base = data_buffer c n ~width:4 in
      gen valid (fun i -> Value.Interval_year_month (Int32.to_int (String.get_int32_le s (base + (4 * i)))))
  | Interval Day_time ->
      let valid = validity c n in
      let base = data_buffer c n ~width:8 in
      gen valid (fun i ->
          let p = base + (8 * i) in
          Value.Interval_day_time (Int32.to_int (String.get_int32_le s p), Int32.to_int (String.get_int32_le s (p + 4))))
  | Interval Month_day_nano ->
      let valid = validity c n in
      let base = data_buffer c n ~width:16 in
      gen valid (fun i ->
          let p = base + (16 * i) in
          Value.Interval_month_day_nano
            ( Int32.to_int (String.get_int32_le s p),
              Int32.to_int (String.get_int32_le s (p + 4)),
              String.get_int64_le s (p + 8) ))
  | List | Large_list ->
      let valid = validity c n in
      let off = if f.typ = List then offsets32 c n else offsets64 c n in
      let child = match f.children with [ ch ] -> ch | _ -> malformed "list field %S must have one child" f.name in
      let values = decode_column c child in
      gen valid (fun i ->
          let a = off i and b = off (i + 1) in
          if a < 0 || b < a || b > Array.length values then malformed "bad list offsets";
          Value.List (Array.sub values a (b - a)))
  | Fixed_size_list k ->
      let valid = validity c n in
      let child =
        match f.children with [ ch ] -> ch | _ -> malformed "fixed-size list field %S must have one child" f.name
      in
      let values = decode_column c child in
      gen valid (fun i -> Value.List (Array.sub values (i * k) k))
  | Struct ->
      let valid = validity c n in
      let cols = List.map (fun ch -> (ch.name, decode_column c ch)) f.children in
      let cols = Array.of_list cols in
      gen valid (fun i -> Value.Struct (Array.map (fun (name, col) -> (name, col.(i))) cols))
  | Map _ ->
      let valid = validity c n in
      let off = offsets32 c n in
      let entries = match f.children with [ e ] -> e | _ -> malformed "map field %S must have one child" f.name in
      (* entries is a struct<key, value>; decode it by hand to get parallel key/value arrays *)
      let en = next_node c in
      let _entries_valid = validity c en in
      let kf, vf =
        match entries.children with [ k; v ] -> (k, v) | _ -> malformed "map entries must have two children"
      in
      let keys = decode_column c kf in
      let vals = decode_column c vf in
      gen valid (fun i ->
          let a = off i and b = off (i + 1) in
          if a < 0 || b < a || b > Array.length keys then malformed "bad map offsets";
          Value.Map (Array.init (b - a) (fun j -> (keys.(a + j), vals.(a + j)))))
  | Other what -> unsupported "arrow type %s (field %S)" what f.name

let parse_record_batch (schema : schema) (t : Fb.table) ~(body : string) ~body_off : batch =
  (match Fb.get_table t 3 with
  | Some comp ->
      let codec = match Fb.get_u8 comp 0 with 0 -> "LZ4_FRAME" | 1 -> "ZSTD" | n -> Printf.sprintf "codec#%d" n in
      unsupported "compressed record batch (%s)" codec
  | None -> ());
  let length = Int64.to_int (Fb.get_i64 t 0) in
  let s = t.Fb.s in
  let nodes =
    List.map
      (fun p -> { length = Int64.to_int (Fb.i64 s p); null_count = Int64.to_int (Fb.i64 s (p + 8)) })
      (Fb.get_struct_vector t 1 ~size:16)
  in
  let buffers =
    List.map
      (fun p -> { off = Int64.to_int (Fb.i64 s p); len = Int64.to_int (Fb.i64 s (p + 8)) })
      (Fb.get_struct_vector t 2 ~size:16)
  in
  let c = { body; body_off; nodes; buffers } in
  let columns = Array.of_list (List.map (decode_column c) schema.fields) in
  { num_rows = length; columns }

(* ---- Stream framing ---- *)

(** Decode a complete Arrow IPC stream held in [data]. Returns the schema and every record batch, in order. A stream
    with no schema message yields [None]. *)
let read_stream ?(off = 0) ?len (data : string) : schema option * batch list =
  let len = match len with Some l -> l | None -> String.length data - off in
  let limit = off + len in
  let schema = ref None in
  let batches = ref [] in
  let pos = ref off in
  let continue = ref true in
  while !continue && !pos + 4 <= limit do
    let c = Fb.u32 data !pos in
    let meta_len =
      if c = 0xFFFF_FFFF then begin
        if !pos + 8 > limit then malformed "truncated continuation marker";
        let l = Fb.i32 data (!pos + 4) in
        pos := !pos + 8;
        l
      end
      else begin
        pos := !pos + 4;
        c
      end
    in
    if meta_len = 0 then continue := false
    else begin
      if !pos + meta_len > limit then malformed "truncated message metadata";
      let msg = Fb.root (String.sub data !pos meta_len) in
      pos := !pos + meta_len;
      let header_type = Fb.get_u8 msg 1 in
      let body_len = Int64.to_int (Fb.get_i64 msg 3) in
      if !pos + body_len > limit then malformed "truncated message body";
      let body_off = !pos in
      pos := !pos + body_len;
      match header_type with
      | 1 -> schema := Option.map parse_schema (Fb.get_table msg 2)
      | 3 -> (
          match (!schema, Fb.get_table msg 2) with
          | Some sc, Some rb -> batches := parse_record_batch sc rb ~body:data ~body_off :: !batches
          | None, _ -> malformed "record batch before schema"
          | _, None -> malformed "record batch message without header")
      | 2 -> unsupported "dictionary batch"
      | _ -> () (* tensors etc.: skip *)
    end
  done;
  (!schema, List.rev !batches)

(** Rows of a batch, as arrays in field order. *)
let rows (b : batch) : Value.t array list =
  let ncols = Array.length b.columns in
  List.init b.num_rows (fun i -> Array.init ncols (fun c -> b.columns.(c).(i)))
