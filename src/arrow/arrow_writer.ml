(* Arrow IPC stream writer: one schema message, one record batch, EOS.

   Enough to ship a local table to Spark Connect ([LocalRelation]). Handles the
   types [Arrow_ipc] reads, minus maps, unions and dictionaries. *)

open Arrow_ipc
module Fb = Flatbuf_builder

exception Unsupported = Arrow_ipc.Unsupported

(* ---- schema inference ---- *)

let rec first_non_null (vs : Value.t array) i =
  if i >= Array.length vs then None else match vs.(i) with Value.Null -> first_non_null vs (i + 1) | v -> Some v

let rec infer_type (vs : Value.t array) : typ * field list =
  match first_non_null vs 0 with
  | None -> (Null, [])
  | Some v -> (
      match v with
      | Value.Null -> (Null, [])
      | Bool _ -> (Bool, [])
      | Int _ | Int64 _ -> (Int { bits = 64; signed = true }, [])
      | Float _ -> (Float { bits = 64 }, [])
      | String _ -> (Utf8, [])
      | Binary _ -> (Binary, [])
      | Date _ -> (Date Day, [])
      | Time (_, unit) -> (Time { unit; bits = 64 }, [])
      | Timestamp (_, unit, tz) -> (Timestamp { unit; tz }, [])
      | Duration (_, unit) -> (Duration unit, [])
      | Interval_year_month _ -> (Interval Year_month, [])
      | Interval_day_time _ -> (Interval Day_time, [])
      | Interval_month_day_nano _ -> (Interval Month_day_nano, [])
      | Decimal _ ->
          let precision = ref 1 and scale = ref 0 in
          Array.iter
            (function
              | Value.Decimal s ->
                  let digits =
                    String.length
                      (String.concat "" (String.split_on_char '.' (String.concat "" (String.split_on_char '-' s))))
                  in
                  let sc = match String.index_opt s '.' with Some i -> String.length s - i - 1 | None -> 0 in
                  precision := max !precision digits;
                  scale := max !scale sc
              | _ -> ())
            vs;
          (Decimal { precision = max !precision !scale; scale = !scale; bits = 128 }, [])
      | List _ ->
          let all = Array.concat (Array.to_list (Array.map (function Value.List xs -> xs | _ -> [||]) vs)) in
          let t, ch = infer_type all in
          (List, [ { name = "element"; nullable = true; typ = t; children = ch; metadata = [] } ])
      | Struct fs ->
          let children =
            Array.to_list
              (Array.mapi
                 (fun i (name, _) ->
                   let col =
                     Array.map (function Value.Struct xs when i < Array.length xs -> snd xs.(i) | _ -> Value.Null) vs
                   in
                   let t, ch = infer_type col in
                   { name; nullable = true; typ = t; children = ch; metadata = [] })
                 fs)
          in
          (Struct, children)
      | Map _ -> raise (Unsupported "writing map columns"))

let infer_field name vs =
  let typ, children = infer_type vs in
  { name; nullable = true; typ; children; metadata = [] }

(* ---- column encoding ---- *)

type encoded = { nodes : (int * int) list; buffers : string list }

let pad8 n = (n + 7) land lnot 7

let bitmap_of n f =
  let b = Bytes.make ((n + 7) / 8) '\000' in
  for i = 0 to n - 1 do
    if f i then Bytes.set_uint8 b (i lsr 3) (Bytes.get_uint8 b (i lsr 3) lor (1 lsl (i land 7)))
  done;
  Bytes.unsafe_to_string b

let validity (vs : Value.t array) =
  let nulls = Array.fold_left (fun acc v -> if v = Value.Null then acc + 1 else acc) 0 vs in
  let bm = if nulls = 0 then "" else bitmap_of (Array.length vs) (fun i -> vs.(i) <> Value.Null) in
  (nulls, bm)

let type_mismatch what (v : Value.t) =
  invalid_arg (Printf.sprintf "Arrow_writer: expected %s, got %s" what (Value.to_string v))

let fixed (vs : Value.t array) width (put : Bytes.t -> int -> Value.t -> unit) =
  let b = Bytes.make (Array.length vs * width) '\000' in
  Array.iteri (fun i v -> if v <> Value.Null then put b (i * width) v) vs;
  Bytes.unsafe_to_string b

let to_unit v unit target =
  let open Value in
  let num = unit_per_second target and den = unit_per_second unit in
  if num >= den then Int64.mul v (Int64.div num den) else Int64.div v (Int64.div den num)

(* decimal string -> little-endian two's complement of [len] bytes *)
let decimal_to_le_bytes s ~scale ~len =
  let neg = String.length s > 0 && s.[0] = '-' in
  let s = if neg || (String.length s > 0 && s.[0] = '+') then String.sub s 1 (String.length s - 1) else s in
  let int_part, frac_part =
    match String.index_opt s '.' with
    | Some i -> (String.sub s 0 i, String.sub s (i + 1) (String.length s - i - 1))
    | None -> (s, "")
  in
  if String.length frac_part > scale then
    invalid_arg (Printf.sprintf "Arrow_writer: decimal %s has more than %d fractional digits" s scale);
  let digits = int_part ^ frac_part ^ String.make (scale - String.length frac_part) '0' in
  (* big-endian magnitude via repeated multiply-add on a byte array *)
  let mag = Bytes.make len '\000' in
  String.iter
    (fun c ->
      if c < '0' || c > '9' then invalid_arg ("Arrow_writer: bad decimal " ^ s);
      let carry = ref (Char.code c - 48) in
      for i = len - 1 downto 0 do
        let v = (Bytes.get_uint8 mag i * 10) + !carry in
        Bytes.set_uint8 mag i (v land 0xff);
        carry := v lsr 8
      done;
      if !carry <> 0 then invalid_arg ("Arrow_writer: decimal overflow " ^ s))
    digits;
  if neg then begin
    (* two's complement negate *)
    let carry = ref 1 in
    for i = len - 1 downto 0 do
      let v = (lnot (Bytes.get_uint8 mag i) land 0xff) + !carry in
      Bytes.set_uint8 mag i (v land 0xff);
      carry := v lsr 8
    done
  end;
  (* to little-endian *)
  let le = Bytes.create len in
  for i = 0 to len - 1 do
    Bytes.set le i (Bytes.get mag (len - 1 - i))
  done;
  Bytes.unsafe_to_string le

let rec encode_column (f : field) (vs : Value.t array) : encoded =
  let n = Array.length vs in
  let nulls, vbm = validity vs in
  let node = (n, nulls) in
  let int_of = function
    | Value.Int i -> Int64.of_int i
    | Value.Int64 i -> i
    | Value.Date d -> Int64.of_int d
    | v -> type_mismatch "integer" v
  in
  match f.typ with
  | Null -> { nodes = [ (n, n) ]; buffers = [] }
  | Bool ->
      let data =
        bitmap_of n (fun i -> match vs.(i) with Value.Bool b -> b | Value.Null -> false | v -> type_mismatch "bool" v)
      in
      { nodes = [ node ]; buffers = [ vbm; data ] }
  | Int { bits; _ } ->
      let w = bits / 8 in
      let data =
        fixed vs w (fun b off v ->
            let x = int_of v in
            match bits with
            | 8 -> Bytes.set_int8 b off (Int64.to_int x)
            | 16 -> Bytes.set_int16_le b off (Int64.to_int x)
            | 32 -> Bytes.set_int32_le b off (Int64.to_int32 x)
            | _ -> Bytes.set_int64_le b off x)
      in
      { nodes = [ node ]; buffers = [ vbm; data ] }
  | Float { bits } ->
      let w = bits / 8 in
      let fl = function
        | Value.Float f -> f
        | Value.Int i -> float_of_int i
        | Value.Int64 i -> Int64.to_float i
        | v -> type_mismatch "float" v
      in
      let data =
        fixed vs w (fun b off v ->
            if bits = 32 then Bytes.set_int32_le b off (Int32.bits_of_float (fl v))
            else Bytes.set_int64_le b off (Int64.bits_of_float (fl v)))
      in
      { nodes = [ node ]; buffers = [ vbm; data ] }
  | Utf8 | Binary ->
      let str = function Value.String s | Value.Binary s -> s | Value.Null -> "" | v -> type_mismatch "string" v in
      let offs = Bytes.create (4 * (n + 1)) in
      let data = Buffer.create 256 in
      Bytes.set_int32_le offs 0 0l;
      Array.iteri
        (fun i v ->
          Buffer.add_string data (str v);
          Bytes.set_int32_le offs (4 * (i + 1)) (Int32.of_int (Buffer.length data)))
        vs;
      { nodes = [ node ]; buffers = [ vbm; Bytes.unsafe_to_string offs; Buffer.contents data ] }
  | Fixed_size_binary w ->
      let data =
        fixed vs w (fun b off v ->
            match v with
            | Value.Binary s when String.length s = w -> Bytes.blit_string s 0 b off w
            | v -> type_mismatch (Printf.sprintf "binary[%d]" w) v)
      in
      { nodes = [ node ]; buffers = [ vbm; data ] }
  | Decimal { scale; bits; _ } ->
      let w = bits / 8 in
      let data =
        fixed vs w (fun b off v ->
            let s =
              match v with
              | Value.Decimal s -> s
              | Value.Int i -> string_of_int i
              | Value.Int64 i -> Int64.to_string i
              | v -> type_mismatch "decimal" v
            in
            Bytes.blit_string (decimal_to_le_bytes s ~scale ~len:w) 0 b off w)
      in
      { nodes = [ node ]; buffers = [ vbm; data ] }
  | Date Day ->
      let data = fixed vs 4 (fun b off v -> Bytes.set_int32_le b off (Int64.to_int32 (int_of v))) in
      { nodes = [ node ]; buffers = [ vbm; data ] }
  | Date Milli ->
      let data = fixed vs 8 (fun b off v -> Bytes.set_int64_le b off (Int64.mul (int_of v) 86_400_000L)) in
      { nodes = [ node ]; buffers = [ vbm; data ] }
  | Time { unit; bits } ->
      let w = bits / 8 in
      let data =
        fixed vs w (fun b off v ->
            let x = match v with Value.Time (x, u) -> to_unit x u unit | v -> int_of v in
            if bits = 32 then Bytes.set_int32_le b off (Int64.to_int32 x) else Bytes.set_int64_le b off x)
      in
      { nodes = [ node ]; buffers = [ vbm; data ] }
  | Timestamp { unit; _ } ->
      let data =
        fixed vs 8 (fun b off v ->
            Bytes.set_int64_le b off (match v with Value.Timestamp (x, u, _) -> to_unit x u unit | v -> int_of v))
      in
      { nodes = [ node ]; buffers = [ vbm; data ] }
  | Duration unit ->
      let data =
        fixed vs 8 (fun b off v ->
            Bytes.set_int64_le b off (match v with Value.Duration (x, u) -> to_unit x u unit | v -> int_of v))
      in
      { nodes = [ node ]; buffers = [ vbm; data ] }
  | Interval Year_month ->
      let data =
        fixed vs 4 (fun b off v ->
            Bytes.set_int32_le b off
              (match v with Value.Interval_year_month m -> Int32.of_int m | v -> Int64.to_int32 (int_of v)))
      in
      { nodes = [ node ]; buffers = [ vbm; data ] }
  | Interval Day_time ->
      let data =
        fixed vs 8 (fun b off v ->
            match v with
            | Value.Interval_day_time (d, ms) ->
                Bytes.set_int32_le b off (Int32.of_int d);
                Bytes.set_int32_le b (off + 4) (Int32.of_int ms)
            | v -> type_mismatch "day_time interval" v)
      in
      { nodes = [ node ]; buffers = [ vbm; data ] }
  | Interval Month_day_nano ->
      let data =
        fixed vs 16 (fun b off v ->
            match v with
            | Value.Interval_month_day_nano (m, d, ns) ->
                Bytes.set_int32_le b off (Int32.of_int m);
                Bytes.set_int32_le b (off + 4) (Int32.of_int d);
                Bytes.set_int64_le b (off + 8) ns
            | v -> type_mismatch "month_day_nano interval" v)
      in
      { nodes = [ node ]; buffers = [ vbm; data ] }
  | List ->
      let child = match f.children with [ c ] -> c | _ -> invalid_arg "Arrow_writer: list field needs one child" in
      let items = function Value.List xs -> xs | Value.Null -> [||] | v -> type_mismatch "list" v in
      let offs = Bytes.create (4 * (n + 1)) in
      Bytes.set_int32_le offs 0 0l;
      let total = ref 0 in
      Array.iteri
        (fun i v ->
          total := !total + Array.length (items v);
          Bytes.set_int32_le offs (4 * (i + 1)) (Int32.of_int !total))
        vs;
      let all = Array.concat (Array.to_list (Array.map items vs)) in
      let ch = encode_column child all in
      { nodes = node :: ch.nodes; buffers = vbm :: Bytes.unsafe_to_string offs :: ch.buffers }
  | Struct ->
      let cols =
        List.mapi
          (fun i (c : field) ->
            let col =
              Array.map
                (function
                  | Value.Struct fs -> (
                      match List.assoc_opt c.name (Array.to_list fs) with
                      | Some v -> v
                      | None -> if i < Array.length fs then snd fs.(i) else Value.Null)
                  | Value.Null -> Value.Null
                  | v -> type_mismatch "struct" v)
                vs
            in
            encode_column c col)
          f.children
      in
      {
        nodes = node :: List.concat_map (fun e -> e.nodes) cols;
        buffers = vbm :: List.concat_map (fun e -> e.buffers) cols;
      }
  | Fixed_size_list _ | Large_utf8 | Large_binary | Large_list | Map _ | Other _ ->
      raise (Unsupported ("writing " ^ typ_to_string f.typ))

(* ---- flatbuffer metadata ---- *)

let time_unit_int = function Second -> 0 | Millisecond -> 1 | Microsecond -> 2 | Nanosecond -> 3

(* Returns (type id, offset of the type table). *)
let build_type b (typ : typ) =
  let empty () =
    Fb.start_table b 0;
    Fb.end_table b
  in
  match typ with
  | Null -> (1, empty ())
  | Int { bits; signed } ->
      Fb.start_table b 2;
      Fb.field_i32 b 0 bits;
      Fb.field_bool b 1 signed;
      (2, Fb.end_table b)
  | Float { bits } ->
      Fb.start_table b 1;
      Fb.field_i16 b 0 (match bits with 16 -> 0 | 32 -> 1 | _ -> 2);
      (3, Fb.end_table b)
  | Binary -> (4, empty ())
  | Utf8 -> (5, empty ())
  | Bool -> (6, empty ())
  | Decimal { precision; scale; bits } ->
      Fb.start_table b 3;
      Fb.field_i32 b 0 precision;
      Fb.field_i32 b 1 scale;
      Fb.field_i32 b 2 bits;
      (7, Fb.end_table b)
  | Date u ->
      Fb.start_table b 1;
      Fb.field_i16 b 0 (match u with Day -> 0 | Milli -> 1);
      (8, Fb.end_table b)
  | Time { unit; bits } ->
      Fb.start_table b 2;
      Fb.field_i16 b 0 (time_unit_int unit);
      Fb.field_i32 b 1 bits;
      (9, Fb.end_table b)
  | Timestamp { unit; tz } ->
      let tz_off = Option.map (Fb.create_string b) tz in
      Fb.start_table b 2;
      Fb.field_i16 b 0 (time_unit_int unit);
      Option.iter (Fb.field_uoffset b 1) tz_off;
      (10, Fb.end_table b)
  | Interval u ->
      Fb.start_table b 1;
      Fb.field_i16 b 0 (match u with Year_month -> 0 | Day_time -> 1 | Month_day_nano -> 2);
      (11, Fb.end_table b)
  | List -> (12, empty ())
  | Struct -> (13, empty ())
  | Fixed_size_binary w ->
      Fb.start_table b 1;
      Fb.field_i32 b 0 w;
      (15, Fb.end_table b)
  | Fixed_size_list k ->
      Fb.start_table b 1;
      Fb.field_i32 b 0 k;
      (16, Fb.end_table b)
  | Map { keys_sorted } ->
      Fb.start_table b 1;
      Fb.field_bool b 0 keys_sorted;
      (17, Fb.end_table b)
  | Duration u ->
      Fb.start_table b 1;
      Fb.field_i16 b 0 (time_unit_int u);
      (18, Fb.end_table b)
  | Large_binary -> (19, empty ())
  | Large_utf8 -> (20, empty ())
  | Large_list -> (21, empty ())
  | Other s -> raise (Unsupported ("writing " ^ s))

let build_metadata b (kvs : (string * string) list) =
  let tables =
    List.map
      (fun (k, v) ->
        let ko = Fb.create_string b k and vo = Fb.create_string b v in
        Fb.start_table b 2;
        Fb.field_uoffset b 0 ko;
        Fb.field_uoffset b 1 vo;
        Fb.end_table b)
      kvs
  in
  Fb.create_offset_vector b tables

let rec build_field b (f : field) =
  let name = Fb.create_string b f.name in
  let children = Fb.create_offset_vector b (List.map (build_field b) f.children) in
  let md = if f.metadata = [] then None else Some (build_metadata b f.metadata) in
  let type_id, type_off = build_type b f.typ in
  Fb.start_table b 7;
  Fb.field_uoffset b 0 name;
  Fb.field_bool b 1 f.nullable;
  Fb.field_u8 b 2 type_id;
  Fb.field_uoffset b 3 type_off;
  Fb.field_uoffset b 5 children;
  Option.iter (Fb.field_uoffset b 6) md;
  Fb.end_table b

let build_message b ~header_type ~header ~body_length =
  Fb.start_table b 5;
  Fb.field_i16 b 0 4;
  (* MetadataVersion V5 *)
  Fb.field_u8 b 1 header_type;
  Fb.field_uoffset b 2 header;
  Fb.field_i64 b 3 body_length;
  Fb.finish b (Fb.end_table b)

let schema_message (schema : schema) =
  let b = Fb.create () in
  let fields = Fb.create_offset_vector b (List.map (build_field b) schema.fields) in
  let md = if schema.schema_metadata = [] then None else Some (build_metadata b schema.schema_metadata) in
  Fb.start_table b 4;
  Fb.field_i16 b 0 0;
  Fb.field_uoffset b 1 fields;
  Option.iter (Fb.field_uoffset b 2) md;
  let schema_off = Fb.end_table b in
  build_message b ~header_type:1 ~header:schema_off ~body_length:0L

let record_batch_message ~num_rows (nodes : (int * int) list) (buffers : (int * int) list) ~body_length =
  let b = Fb.create () in
  let nb = List.length buffers in
  Fb.start_vector b ~elem_size:16 ~n:nb ~alignment:8;
  List.iter
    (fun (off, len) ->
      Fb.add_i64 b (Int64.of_int len);
      Fb.add_i64 b (Int64.of_int off))
    (List.rev buffers);
  let buffers_off = Fb.end_vector b nb in
  let nn = List.length nodes in
  Fb.start_vector b ~elem_size:16 ~n:nn ~alignment:8;
  List.iter
    (fun (len, nulls) ->
      Fb.add_i64 b (Int64.of_int nulls);
      Fb.add_i64 b (Int64.of_int len))
    (List.rev nodes);
  let nodes_off = Fb.end_vector b nn in
  Fb.start_table b 5;
  Fb.field_i64 b 0 (Int64.of_int num_rows);
  Fb.field_uoffset b 1 nodes_off;
  Fb.field_uoffset b 2 buffers_off;
  let rb = Fb.end_table b in
  build_message b ~header_type:3 ~header:rb ~body_length:(Int64.of_int body_length)

(* ---- framing ---- *)

let frame out (meta : string) (body : string list) =
  let padded = pad8 (String.length meta) in
  Buffer.add_string out "\xFF\xFF\xFF\xFF";
  let len = Bytes.create 4 in
  Bytes.set_int32_le len 0 (Int32.of_int padded);
  Buffer.add_bytes out len;
  Buffer.add_string out meta;
  Buffer.add_string out (String.make (padded - String.length meta) '\000');
  List.iter
    (fun buf ->
      Buffer.add_string out buf;
      Buffer.add_string out (String.make (pad8 (String.length buf) - String.length buf) '\000'))
    body

(** Encode columns (one [Value.t array] per field, equal lengths) as an Arrow IPC stream. *)
let encode_columns (schema : schema) (columns : Value.t array list) : string =
  if List.length columns <> List.length schema.fields then
    invalid_arg "Arrow_writer.encode_columns: column count differs from schema";
  let num_rows = match columns with c :: _ -> Array.length c | [] -> 0 in
  List.iter
    (fun c -> if Array.length c <> num_rows then invalid_arg "Arrow_writer.encode_columns: ragged columns")
    columns;
  let encoded = List.map2 encode_column schema.fields columns in
  let nodes = List.concat_map (fun e -> e.nodes) encoded in
  let bufs = List.concat_map (fun e -> e.buffers) encoded in
  let off = ref 0 in
  let layout =
    List.map
      (fun s ->
        let o = !off in
        off := !off + pad8 (String.length s);
        (o, String.length s))
      bufs
  in
  let body_length = !off in
  let out = Buffer.create (1024 + body_length) in
  frame out (schema_message schema) [];
  frame out (record_batch_message ~num_rows nodes layout ~body_length) bufs;
  Buffer.add_string out "\xFF\xFF\xFF\xFF\x00\x00\x00\x00";
  Buffer.contents out

(** Encode rows. [schema] overrides inference; [columns] names the fields. *)
let encode ?(schema : field list option) ~(columns : string list) (rows : Value.t list list) : string =
  let ncols = List.length columns in
  let cols =
    List.init ncols (fun c ->
        Array.of_list (List.map (fun r -> match List.nth_opt r c with Some v -> v | None -> Value.Null) rows))
  in
  let fields =
    match schema with
    | Some fs -> if List.length fs <> ncols then invalid_arg "Arrow_writer.encode: schema/columns mismatch" else fs
    | None -> List.map2 infer_field columns cols
  in
  encode_columns { fields; schema_metadata = [] } cols
