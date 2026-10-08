(* Spark SQL data types (the protobuf [DataType]) rendered as DDL-ish strings. *)

module T = Spark_connect_proto.Types

type t = T.data_type

let interval_field = function
  | 0 -> "year"
  | 1 -> "month"
  | 2 -> "day"
  | 3 -> "hour"
  | 4 -> "minute"
  | 5 -> "second"
  | n -> string_of_int n

let rec simple_string (t : t) =
  match t with
  | T.Null _ -> "void"
  | Binary _ -> "binary"
  | Boolean _ -> "boolean"
  | Byte _ -> "tinyint"
  | Short _ -> "smallint"
  | Integer _ -> "int"
  | Long _ -> "bigint"
  | Float _ -> "float"
  | Double _ -> "double"
  | Decimal d -> Printf.sprintf "decimal(%ld,%ld)" d.precision d.scale
  | String s ->
      if s.collation = "" || s.collation = "UTF8_BINARY" then "string"
      else Printf.sprintf "string collate %s" s.collation
  | Char c -> Printf.sprintf "char(%ld)" c.length
  | Var_char c -> Printf.sprintf "varchar(%ld)" c.length
  | Date _ -> "date"
  | Timestamp _ -> "timestamp"
  | Timestamp_ntz _ -> "timestamp_ntz"
  | Calendar_interval _ -> "interval"
  | Year_month_interval i ->
      if i.start_field = i.end_field then Printf.sprintf "interval %s" (interval_field (Int32.to_int i.start_field))
      else
        Printf.sprintf "interval %s to %s"
          (interval_field (Int32.to_int i.start_field))
          (interval_field (Int32.to_int i.end_field))
  | Day_time_interval i ->
      if i.start_field = i.end_field then Printf.sprintf "interval %s" (interval_field (Int32.to_int i.start_field))
      else
        Printf.sprintf "interval %s to %s"
          (interval_field (Int32.to_int i.start_field))
          (interval_field (Int32.to_int i.end_field))
  | Array a -> Printf.sprintf "array<%s>" (opt a.element_type)
  | Struct s ->
      "struct<"
      ^ String.concat "," (List.map (fun (f : T.data_type_struct_field) -> f.name ^ ":" ^ opt f.data_type) s.fields)
      ^ ">"
  | Map m -> Printf.sprintf "map<%s,%s>" (opt m.key_type) (opt m.value_type)
  | Variant _ -> "variant"
  | Udt u -> if u.type_ <> "" then u.type_ else "udt"
  | Geometry _ -> "geometry"
  | Geography _ -> "geography"
  | Unparsed u -> u.data_type_string
  | Time t -> Printf.sprintf "time(%ld)" t.precision

and opt = function Some t -> simple_string t | None -> "?"

(** Fields of a struct type, or [] for any other type. *)
let fields (t : t) =
  match t with
  | T.Struct s -> List.map (fun (f : T.data_type_struct_field) -> (f.name, f.data_type, f.nullable)) s.fields
  | _ -> []

(** Multi-line tree rendering in the style of [DataFrame.printSchema]. *)
let tree_string (t : t) =
  let b = Buffer.create 256 in
  Buffer.add_string b "root\n";
  let rec go indent (name, dt, nullable) =
    Buffer.add_string b (Printf.sprintf "%s|-- %s: %s (nullable = %b)\n" indent name (opt dt) nullable);
    match dt with
    | Some (T.Struct s) ->
        List.iter
          (fun (f : T.data_type_struct_field) -> go (indent ^ "|    ") (f.name, f.data_type, f.nullable))
          s.fields
    | Some (T.Array a) -> go (indent ^ "|    ") ("element", a.element_type, a.contains_null)
    | Some (T.Map m) ->
        go (indent ^ "|    ") ("key", m.key_type, false);
        go (indent ^ "|    ") ("value", m.value_type, m.value_contains_null)
    | _ -> ()
  in
  List.iter (go " ") (fields t);
  Buffer.contents b

let null () = T.Null (T.make_data_type_null ())
let boolean () = T.Boolean (T.make_data_type_boolean ())
let byte () = T.Byte (T.make_data_type_byte ())
let short () = T.Short (T.make_data_type_short ())
let integer () = T.Integer (T.make_data_type_integer ())
let long () = T.Long (T.make_data_type_long ())
let float () = T.Float (T.make_data_type_float ())
let double () = T.Double (T.make_data_type_double ())
let string () = T.String (T.make_data_type_string ())
let binary () = T.Binary (T.make_data_type_binary ())
let date () = T.Date (T.make_data_type_date ())
let timestamp () = T.Timestamp (T.make_data_type_timestamp ())
let timestamp_ntz () = T.Timestamp_ntz (T.make_data_type_timestamp_ntz ())

let decimal ~precision ~scale =
  T.Decimal (T.make_data_type_decimal ~precision:(Int32.of_int precision) ~scale:(Int32.of_int scale) ())

let array ?(contains_null = true) el = T.Array (T.make_data_type_array ~element_type:el ~contains_null ())

let map ?(value_contains_null = true) k v =
  T.Map (T.make_data_type_map ~key_type:k ~value_type:v ~value_contains_null ())

let struct_ fields =
  T.Struct
    (T.make_data_type_struct
       ~fields:
         (List.map (fun (name, dt, nullable) -> T.make_data_type_struct_field ~name ~data_type:dt ~nullable ()) fields)
       ())
