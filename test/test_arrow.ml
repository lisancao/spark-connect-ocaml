open Spark_connect_arrow

let read_file f =
  let ic = open_in_bin f in
  let n = in_channel_length ic in
  let s = really_input_string ic n in
  close_in ic;
  s

let fixture name = read_file (Filename.concat "fixtures" (name ^ ".arrow"))
let col (b : Arrow_ipc.batch) i = b.columns.(i)
let v = Value.to_string

let test_types () =
  let schema, batches = Arrow_ipc.read_stream (fixture "types") in
  let schema = Option.get schema in
  let names = List.map (fun (f : Arrow_ipc.field) -> f.name) schema.fields in
  Alcotest.(check int) "field count" 27 (List.length names);
  let b = List.hd batches in
  Alcotest.(check int) "rows" 3 b.num_rows;
  let c name =
    col b
      (let rec idx i = function [] -> raise Not_found | n :: r -> if n = name then i else idx (i + 1) r in
       idx 0 names)
  in
  let check name expected = Alcotest.(check (list string)) name expected (Array.to_list (Array.map v (c name))) in
  check "i8" [ "1"; "NULL"; "-128" ];
  check "i16" [ "300"; "NULL"; "-32768" ];
  check "i32" [ "70000"; "NULL"; "-2147483648" ];
  check "i64" [ "1099511627776"; "NULL"; "-4611686018427387904" ];
  check "u8" [ "255"; "NULL"; "0" ];
  check "u32" [ "4000000000"; "NULL"; "7" ];
  check "f32" [ "1.5"; "NULL"; "-0.25" ];
  check "f64" [ "2.5"; "NULL"; "1.0000000000000001e+300" ];
  check "b" [ "true"; "NULL"; "false" ];
  check "s" [ "hello"; "NULL"; "wörld" ];
  check "bin" [ "0x0001"; "NULL"; "0x" ];
  check "ls" [ "big"; "NULL"; "x" ];
  check "dec" [ "123.45"; "NULL"; "-0.01" ];
  check "dec256" [ "-12345678901234567890123456789012345678.1"; "NULL"; "0.0" ];
  check "d32" [ "2024-02-29"; "NULL"; "1969-12-31" ];
  check "ts_us_utc" [ "2024-01-02 03:04:05.678901"; "NULL"; "1960-01-01 00:00:00.000000" ];
  check "ts_ns" [ "1970-01-01 00:00:00.000000001"; "NULL"; "1969-12-31 23:59:59.999999999" ];
  check "dur" [ "1000ms"; "NULL"; "-5ms" ];
  check "t64" [ "01:01:01.000001"; "NULL"; "00:00:00.000000" ];
  check "fsb" [ "0x61626364"; "NULL"; "0x7778797A" ];
  check "lst" [ "[1, NULL, 3]"; "NULL"; "[]" ];
  check "st" [ "{a: 1, b: x}"; "NULL"; "{a: NULL, b: z}" ];
  check "mp" [ "{k1 -> 1, k2 -> NULL}"; "NULL"; "{}" ];
  check "nested" [ "[[1.0], [2.0, NULL]]"; "NULL"; "[[]]" ];
  check "nul" [ "NULL"; "NULL"; "NULL" ];
  check "fsl" [ "[1, 2]"; "NULL"; "[3, 4]" ];
  check "ym" [ "INTERVAL 1 months 2 days 3 ns"; "NULL"; "INTERVAL 0 months 0 days 0 ns" ];
  (* typed access *)
  Alcotest.(check (option int)) "to_int_opt" (Some 1) (Value.to_int_opt (c "i8").(0));
  Alcotest.(check (option int)) "to_int_opt null" None (Value.to_int_opt (c "i8").(1));
  (* schema rendering *)
  let s = Arrow_ipc.schema_to_string schema in
  Alcotest.(check bool)
    "schema mentions decimal" true
    (String.length s > 0
    &&
      try
        ignore (Str.search_forward (Str.regexp_string "dec: decimal128(10,2)") s 0);
        true
      with Not_found -> false)

let test_multibatch () =
  let schema, batches = Arrow_ipc.read_stream (fixture "multibatch") in
  Alcotest.(check int) "fields" 2 (List.length (Option.get schema).fields);
  Alcotest.(check int) "batches" 4 (List.length batches);
  let total = List.fold_left (fun a (b : Arrow_ipc.batch) -> a + b.num_rows) 0 batches in
  Alcotest.(check int) "rows" 100000 total;
  let last = List.nth batches 3 in
  Alcotest.(check string) "last id" "99999" (v (col last 0).(last.num_rows - 1));
  Alcotest.(check string) "last s" "99999" (v (col last 1).(last.num_rows - 1))

let test_empty () =
  let schema, batches = Arrow_ipc.read_stream (fixture "empty") in
  Alcotest.(check int) "fields" 2 (List.length (Option.get schema).fields);
  Alcotest.(check int) "rows" 0 (List.fold_left (fun a (b : Arrow_ipc.batch) -> a + b.num_rows) 0 batches)

let test_legacy () =
  let _, batches = Arrow_ipc.read_stream (fixture "legacy") in
  Alcotest.(check int) "rows" 10 (List.fold_left (fun a (b : Arrow_ipc.batch) -> a + b.num_rows) 0 batches)

let test_unsupported () =
  Alcotest.check_raises "dictionary" (Arrow_ipc.Unsupported "dictionary-encoded field \"d\"") (fun () ->
      ignore (Arrow_ipc.read_stream (fixture "dict")));
  Alcotest.check_raises "lz4" (Arrow_ipc.Unsupported "compressed record batch (LZ4_FRAME)") (fun () ->
      ignore (Arrow_ipc.read_stream (fixture "lz4")))

let test_malformed () =
  let data = fixture "legacy" in
  let truncated = String.sub data 0 (String.length data - 40) in
  Alcotest.(check bool)
    "truncated raises" true
    (try
       ignore (Arrow_ipc.read_stream truncated);
       false
     with Arrow_ipc.Malformed _ | Flatbuf.Malformed _ -> true);
  Alcotest.(check bool)
    "garbage raises" true
    (try
       ignore (Arrow_ipc.read_stream "\xFF\xFF\xFF\xFF\x10\x00\x00\x00garbagegarbagegarbage");
       false
     with Arrow_ipc.Malformed _ | Flatbuf.Malformed _ -> true)

let roundtrip ?schema columns rows =
  let data = Arrow_writer.encode ?schema ~columns rows in
  let schema, batches = Arrow_ipc.read_stream data in
  (Option.get schema, List.concat_map Arrow_ipc.rows batches)

let test_roundtrip () =
  let rows =
    [
      [
        Value.Int 1;
        Value.String "a";
        Value.Float 0.5;
        Value.Bool true;
        Value.Date 1;
        Value.Decimal "1.50";
        Value.List [| Value.Int 1 |];
        Value.Struct [| ("k", Value.String "v") |];
        Value.Timestamp (123456789L, Value.Microsecond, Some "UTC");
      ];
      [ Value.Null; Value.Null; Value.Null; Value.Null; Value.Null; Value.Null; Value.Null; Value.Null; Value.Null ];
      [
        Value.Int64 (-3L);
        Value.String "";
        Value.Float nan;
        Value.Bool false;
        Value.Date (-1);
        Value.Decimal "-99.99";
        Value.List [||];
        Value.Struct [| ("k", Value.Null) |];
        Value.Timestamp (0L, Value.Microsecond, Some "UTC");
      ];
    ]
  in
  let schema, out = roundtrip [ "i"; "s"; "f"; "b"; "d"; "dec"; "l"; "st"; "ts" ] rows in
  Alcotest.(check string)
    "schema"
    "i: int64\n\
     s: utf8\n\
     f: float64\n\
     b: bool\n\
     d: date32\n\
     dec: decimal128(4,2)\n\
     l: list<element: int64>\n\
     st: struct<k: utf8>\n\
     ts: timestamp[us, UTC]"
    (Arrow_ipc.schema_to_string schema);
  let expect = List.map (fun r -> String.concat "|" (List.map v r)) rows in
  let got = List.map (fun r -> String.concat "|" (Array.to_list (Array.map v r))) out in
  Alcotest.(check (list string)) "rows" expect got

let test_roundtrip_explicit_schema () =
  let open Arrow_ipc in
  let f name typ = { name; nullable = true; typ; children = []; metadata = [] } in
  let schema =
    [
      f "a" (Int { bits = 32; signed = true });
      f "b" (Float { bits = 32 });
      f "c" Binary;
      f "d" (Decimal { precision = 38; scale = 10; bits = 128 });
      f "e" (Timestamp { unit = Millisecond; tz = None });
    ]
  in
  let rows =
    [
      [
        Value.Int 7;
        Value.Float 1.25;
        Value.Binary "\000\255";
        Value.Decimal "-1234567890123456789.0123456789";
        Value.Timestamp (1_000L, Value.Second, None);
      ];
    ]
  in
  let sc, out = roundtrip ~schema [ "a"; "b"; "c"; "d"; "e" ] rows in
  Alcotest.(check string)
    "schema" "a: int32\nb: float32\nc: binary\nd: decimal128(38,10)\ne: timestamp[ms]" (schema_to_string sc);
  Alcotest.(check (list string))
    "row"
    [ "7"; "1.25"; "0x00FF"; "-1234567890123456789.0123456789"; "1970-01-01 00:16:40.000" ]
    (Array.to_list (Array.map v (List.hd out)))

let test_value_formatting () =
  Alcotest.(check string) "date epoch" "1970-01-01" (Value.date_to_string 0);
  Alcotest.(check string) "date neg" "1969-12-31" (Value.date_to_string (-1));
  Alcotest.(check string) "date leap" "2000-02-29" (Value.date_to_string 11016);
  Alcotest.(check int) "days_of_civil" 11016 (Value.days_of_civil 2000 2 29);
  Alcotest.(check string)
    "ts neg micros" "1969-12-31 23:59:59.999999"
    (Value.timestamp_to_string (-1L) Value.Microsecond);
  Alcotest.(check string)
    "decimal zero" "0.00"
    (Value.decimal_of_le_bytes (String.make 16 '\000') ~off:0 ~len:16 ~scale:2);
  Alcotest.(check string)
    "decimal -1 scale 0" "-1"
    (Value.decimal_of_le_bytes (String.make 16 '\255') ~off:0 ~len:16 ~scale:0);
  Alcotest.(check string)
    "decimal small scale" "0.005"
    (Value.decimal_of_le_bytes ("\005" ^ String.make 15 '\000') ~off:0 ~len:16 ~scale:3)

let () =
  Alcotest.run "arrow"
    [
      ( "reader",
        [
          Alcotest.test_case "all types" `Quick test_types;
          Alcotest.test_case "multi batch" `Quick test_multibatch;
          Alcotest.test_case "empty" `Quick test_empty;
          Alcotest.test_case "legacy framing" `Quick test_legacy;
          Alcotest.test_case "unsupported" `Quick test_unsupported;
          Alcotest.test_case "malformed" `Quick test_malformed;
        ] );
      ( "writer",
        [
          Alcotest.test_case "round trip inferred" `Quick test_roundtrip;
          Alcotest.test_case "round trip explicit schema" `Quick test_roundtrip_explicit_schema;
        ] );
      ("value", [ Alcotest.test_case "formatting" `Quick test_value_formatting ]);
    ]
