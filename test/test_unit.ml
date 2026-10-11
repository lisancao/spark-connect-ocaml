open Spark_connect

let test_connection_string () =
  let c = Session.parse_connection_string "sc://localhost" in
  Alcotest.(check string) "host" "localhost" c.host;
  Alcotest.(check int) "default port" 15002 c.port;
  Alcotest.(check bool) "no ssl" false c.use_ssl;
  let c =
    Session.parse_connection_string
      "sc://spark.example.com:443/;token=abc;user_id=lisa;x-custom-header=123;session_id=s1"
  in
  Alcotest.(check string) "host" "spark.example.com" c.host;
  Alcotest.(check int) "port" 443 c.port;
  Alcotest.(check bool) "token implies ssl" true c.use_ssl;
  Alcotest.(check (option string)) "token" (Some "abc") c.token;
  Alcotest.(check (option string)) "user" (Some "lisa") c.user_id;
  Alcotest.(check (option string)) "session" (Some "s1") c.session_id;
  Alcotest.(check (list (pair string string))) "headers" [ ("x-custom-header", "123") ] c.headers;
  let c = Session.parse_connection_string "sc://h:1/;token=t;use_ssl=false" in
  Alcotest.(check bool) "ssl override" false c.use_ssl;
  Alcotest.(check bool)
    "bad scheme" true
    (try
       ignore (Session.parse_connection_string "http://x");
       false
     with Invalid_argument _ -> true)

let test_grpc_helpers () =
  Alcotest.(check string) "percent" "a b%c" (Grpc_client.percent_decode "a%20b%c");
  Alcotest.(check string) "base64" "hello" (Grpc_client.base64_decode "aGVsbG8=");
  Alcotest.(check string) "base64 nopad" "hello" (Grpc_client.base64_decode "aGVsbG8");
  Alcotest.(check string) "base64 url" "\xfb\xff" (Grpc_client.base64_decode "-_8");
  let sp = Grpc_client.new_splitter () in
  let got = ref [] in
  let emit m = got := m :: !got in
  let framed = Grpc_client.frame "abc" ^ Grpc_client.frame "" ^ Grpc_client.frame "defgh" in
  (* feed in awkward chunks *)
  Grpc_client.feed sp (String.sub framed 0 3) emit;
  Grpc_client.feed sp (String.sub framed 3 7) emit;
  Grpc_client.feed sp (String.sub framed 10 (String.length framed - 10)) emit;
  Alcotest.(check (list string)) "messages" [ "abc"; ""; "defgh" ] (List.rev !got)

let test_error_parse () =
  Alcotest.(check (option string))
    "class" (Some "DIVIDE_BY_ZERO")
    (Error.parse_error_class "[DIVIDE_BY_ZERO] Division by zero.");
  Alcotest.(check (option string)) "no class" None (Error.parse_error_class "plain message");
  Alcotest.(check (list string)) "classes json" [ "a.B"; "c.D" ] (Error.parse_classes {|["a.B", "c.D"]|})

let test_datatype () =
  let dt =
    Datatype.struct_
      [
        ("id", Datatype.long (), false);
        ("tags", Datatype.array (Datatype.string ()), true);
        ("m", Datatype.map (Datatype.string ()) (Datatype.decimal ~precision:10 ~scale:2), true);
      ]
  in
  Alcotest.(check string)
    "simple" "struct<id:bigint,tags:array<string>,m:map<string,decimal(10,2)>>" (Datatype.simple_string dt);
  Alcotest.(check string)
    "tree"
    "root\n\
    \ |-- id: bigint (nullable = false)\n\
    \ |-- tags: array<string> (nullable = true)\n\
    \ |    |-- element: string (nullable = true)\n\
    \ |-- m: map<string,decimal(10,2)> (nullable = true)\n\
    \ |    |-- key: string (nullable = false)\n\
    \ |    |-- value: decimal(10,2) (nullable = true)\n"
    (Datatype.tree_string dt)

let test_column_literals () =
  let open Proto.Expressions in
  let lit v = match (Column.lit v).expr_type with Some (Literal l) -> l.literal_type | _ -> None in
  Alcotest.(check bool) "int32" true (lit (Value.Int 5) = Some (Integer 5l));
  Alcotest.(check bool) "int64 promotes" true (lit (Value.Int 5_000_000_000) = Some (Long 5_000_000_000L));
  Alcotest.(check bool) "string" true (lit (Value.String "x") = Some (String "x"));
  (match lit (Value.Decimal "-12.345") with
  | Some (Decimal d) -> Alcotest.(check (pair int32 int32)) "decimal shape" (5l, 3l) (d.precision, d.scale)
  | _ -> Alcotest.fail "decimal literal");
  (match Column.O.(Column.col "a" + Column.int 1).expr_type with
  | Some (Unresolved_function f) -> Alcotest.(check string) "op name" "+" f.function_name
  | _ -> Alcotest.fail "operator");
  match
    (Column.otherwise
       (Column.add_when (Column.when_ (Column.col "c") (Column.int 1)) (Column.col "d") (Column.int 2))
       (Column.int 3))
      .expr_type
  with
  | Some (Unresolved_function f) -> Alcotest.(check int) "when args" 5 (List.length f.arguments)
  | _ -> Alcotest.fail "when chain"

let test_sort_order_defaults () =
  let open Proto.Expressions in
  let nulls c = match (Column.to_sort_order c : expression_sort_order) with { null_ordering; _ } -> null_ordering in
  (* Spark's defaults (and what PySpark sends): nulls first ascending, nulls last descending *)
  Alcotest.(check bool) "plain column -> nulls first" true (nulls (Column.col "a") = Sort_nulls_first);
  Alcotest.(check bool) "asc -> nulls first" true (nulls (Column.asc (Column.col "a")) = Sort_nulls_first);
  Alcotest.(check bool) "desc -> nulls last" true (nulls (Column.desc (Column.col "a")) = Sort_nulls_last);
  Alcotest.(check bool) "explicit asc nulls last" true (nulls (Column.asc_nulls_last (Column.col "a")) = Sort_nulls_last)

let () =
  Alcotest.run "unit"
    [
      ("session", [ Alcotest.test_case "connection string" `Quick test_connection_string ]);
      ("grpc", [ Alcotest.test_case "helpers" `Quick test_grpc_helpers ]);
      ("error", [ Alcotest.test_case "parse" `Quick test_error_parse ]);
      ("datatype", [ Alcotest.test_case "render" `Quick test_datatype ]);
      ( "column",
        [
          Alcotest.test_case "literals and ops" `Quick test_column_literals;
          Alcotest.test_case "sort order null defaults" `Quick test_sort_order_defaults;
        ] );
    ]
