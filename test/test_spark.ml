(* Integration tests against a live Spark Connect server. Skipped unless
   SPARK_REMOTE is set (e.g. sc://localhost:15002). *)

open Lwt.Infix
open Spark_connect

let remote = Sys.getenv_opt "SPARK_REMOTE"
let run f = Lwt_main.run (f ())
let spark = lazy (Lwt_main.run (Session.connect (Option.get remote)))
let s () = Lazy.force spark
let strs rows = List.map Row.to_string rows
let check_rows name expected rows = Alcotest.(check (list string)) name expected (strs rows)

let test_version () =
  run (fun () ->
      Session.version (s ()) >|= fun v -> Alcotest.(check bool) "4.x" true (String.length v > 0 && v.[0] = '4'))

let test_sql_types () =
  run (fun () ->
      Session.sql (s ())
        {|SELECT 1Y AS y, 2S AS sh, 3 AS i, 4L AS l, CAST(1.5 AS FLOAT) AS f, 2.5D AS d, 'x' AS str, true AS b,
                 CAST('ab' AS BINARY) AS bin, DATE'2024-02-29' AS dt, TIMESTAMP'2024-01-02 03:04:05.678' AS ts,
                 TIMESTAMP_NTZ'2024-01-02 03:04:05' AS ntz, CAST(-12.345 AS DECIMAL(10,3)) AS dec,
                 ARRAY(1, NULL, 3) AS arr, MAP('k', 1) AS m, NAMED_STRUCT('a', 1, 'b', 'z') AS st, NULL AS n,
                 INTERVAL '1-2' YEAR TO MONTH AS ym, INTERVAL '1 02:03:04.5' DAY TO SECOND AS dts|}
      >>= Dataframe.collect
      >|= fun rows ->
      check_rows "values"
        [
          "[1, 2, 3, 4, 1.5, 2.5, x, true, 0x6162, 2024-02-29, 2024-01-02 03:04:05.678000, 2024-01-02 03:04:05.000000, \
           -12.345, [1, NULL, 3], {k -> 1}, {a: 1, b: z}, NULL, INTERVAL '1-2' YEAR TO MONTH, 93784500000us]";
        ]
        rows;
      let r = List.hd rows in
      Alcotest.(check (list string))
        "names"
        [
          "y"; "sh"; "i"; "l"; "f"; "d"; "str"; "b"; "bin"; "dt"; "ts"; "ntz"; "dec"; "arr"; "m"; "st"; "n"; "ym"; "dts";
        ]
        (Array.to_list (Row.names r));
      (match Row.field r "ts" with
      | Value.Timestamp (_, Value.Microsecond, Some tz) -> Alcotest.(check bool) "tz" true (tz <> "")
      | _ -> Alcotest.fail "ts type");
      (match Row.field r "ntz" with Value.Timestamp (_, _, None) -> () | _ -> Alcotest.fail "ntz type");
      Alcotest.(check int) "y int" 1 (Value.to_int (Row.field r "y")))

let test_dataframe_ops () =
  run (fun () ->
      let sp = s () in
      let open Column.O in
      let df = Session.range ~num_partitions:2 sp 20L in
      let df =
        Dataframe.with_columns df [ ("g", Column.col "id" % Column.int 3); ("sq", Column.col "id" * Column.col "id") ]
      in
      let agg =
        Dataframe.group_by df [ Column.col "g" ] |> fun g ->
        Dataframe.Grouped.agg g
          [ F.count_all () @= "n"; F.sum (Column.col "sq") @= "s"; F.max (Column.col "id") @= "mx" ]
      in
      Dataframe.collect (Dataframe.sort agg [ Column.col "g" ]) >>= fun rows ->
      check_rows "group by" [ "[0, 7, 819, 18]"; "[1, 7, 952, 19]"; "[2, 6, 699, 17]" ] rows;
      Dataframe.count (Dataframe.filter df (Column.col "id" >= Column.int 15 && not (Column.col "g" = Column.int 0)))
      >>= fun n ->
      Alcotest.(check int64) "filter count" 3L n;
      Dataframe.collect
        (Dataframe.select (Dataframe.limit (Dataframe.sort df [ Column.desc (Column.col "id") ]) 2) [ Column.col "id" ])
      >>= fun rows ->
      check_rows "sort desc limit" [ "[19]"; "[18]" ] rows;
      Dataframe.collect
        (Dataframe.offset (Dataframe.sort df [ Column.col "id" ]) 18 |> fun d -> Dataframe.select_names d [ "id" ])
      >>= fun rows ->
      check_rows "offset" [ "[18]"; "[19]" ] rows;
      Dataframe.columns (Dataframe.drop df [ "sq" ]) >>= fun cols ->
      Alcotest.(check (list string)) "drop" [ "id"; "g" ] cols;
      Dataframe.columns (Dataframe.with_column_renamed df "sq" "square") >>= fun cols ->
      Alcotest.(check (list string)) "rename" [ "id"; "g"; "square" ] cols;
      Dataframe.count (Dataframe.distinct (Dataframe.select_names df [ "g" ])) >>= fun n ->
      Alcotest.(check int64) "distinct" 3L n;
      Dataframe.count (Dataframe.drop_duplicates df [ "g" ]) >>= fun n ->
      Alcotest.(check int64) "drop_duplicates" 3L n;
      let a = Dataframe.select_names (Session.range sp 3L) [ "id" ]
      and b = Dataframe.select_names (Session.range ~start:2L sp 5L) [ "id" ] in
      Dataframe.count (Dataframe.union a b) >>= fun n ->
      Alcotest.(check int64) "union" 6L n;
      Dataframe.collect (Dataframe.sort (Dataframe.intersect a b) [ Column.col "id" ]) >>= fun rows ->
      check_rows "intersect" [ "[2]" ] rows;
      Dataframe.collect (Dataframe.sort (Dataframe.except a b) [ Column.col "id" ]) >>= fun rows ->
      check_rows "except" [ "[0]"; "[1]" ] rows;
      Dataframe.collect (Dataframe.select_expr (Session.range sp 1L) [ "id + 41 AS answer"; "upper('x') AS u" ])
      >>= fun rows ->
      check_rows "select_expr" [ "[41, X]" ] rows;
      Dataframe.head (Session.range sp 0L) >>= fun h ->
      Alcotest.(check bool) "empty head" true (Option.is_none h);
      Dataframe.is_empty (Session.range sp 0L) >|= fun e -> Alcotest.(check bool) "is_empty" true e)

let test_join () =
  run (fun () ->
      let sp = s () in
      let people =
        Session.create_dataframe sp ~columns:[ "id"; "name" ]
          [
            [ Value.Int 1; Value.String "ann" ]; [ Value.Int 2; Value.String "bob" ]; [ Value.Int 3; Value.String "cy" ];
          ]
      in
      let orders =
        Session.create_dataframe sp ~columns:[ "pid"; "amount" ]
          [ [ Value.Int 1; Value.Float 10.0 ]; [ Value.Int 1; Value.Float 5.5 ]; [ Value.Int 3; Value.Float 1.0 ] ]
      in
      let joined = Dataframe.join people orders ~on:Column.O.(Column.col "id" = Column.col "pid") in
      let per =
        Dataframe.group_by joined [ Column.col "name" ] |> fun g ->
        Dataframe.Grouped.agg g [ Column.alias (F.sum (Column.col "amount")) "total" ]
      in
      Dataframe.collect (Dataframe.sort per [ Column.col "name" ]) >>= fun rows ->
      check_rows "inner join agg" [ "[ann, 15.5]"; "[cy, 1.0]" ] rows;
      let left = Dataframe.join ~how:`Left_outer people orders ~on:Column.O.(Column.col "id" = Column.col "pid") in
      Dataframe.collect
        (Dataframe.sort (Dataframe.select_names left [ "name"; "amount" ]) [ Column.col "name"; Column.col "amount" ])
      >>= fun rows ->
      check_rows "left join" [ "[ann, 5.5]"; "[ann, 10.0]"; "[bob, NULL]"; "[cy, 1.0]" ] rows;
      let a = Dataframe.alias people "a" and b = Dataframe.alias people "b" in
      let self = Dataframe.join a b ~on:Column.O.(Dataframe.col a "id" = Dataframe.col b "id" + Column.int 1) in
      Dataframe.collect
        (Dataframe.sort (Dataframe.select self [ Column.col "a.name"; Column.col "b.name" ]) [ Column.col "a.name" ])
      >>= fun rows ->
      check_rows "self join with plan ids" [ "[bob, ann]"; "[cy, bob]" ] rows;
      Dataframe.count (Dataframe.join_using people (Dataframe.with_column_renamed orders "pid" "id") [ "id" ])
      >>= fun n ->
      Alcotest.(check int64) "join using" 3L n;
      Dataframe.count (Dataframe.cross_join people orders) >|= fun n -> Alcotest.(check int64) "cross" 9L n)

let test_sql_params_views () =
  run (fun () ->
      let sp = s () in
      let df =
        Session.create_dataframe sp ~columns:[ "k"; "v" ]
          [ [ Value.String "a"; Value.Int 1 ]; [ Value.String "b"; Value.Int 2 ] ]
      in
      Dataframe.create_or_replace_temp_view df "kv" >>= fun () ->
      Session.sql ~args:[ ("want", Value.String "b") ] sp "SELECT v FROM kv WHERE k = :want" >>= Dataframe.collect
      >>= fun rows ->
      check_rows "named param" [ "[2]" ] rows;
      Session.sql ~pos_args:[ Value.Int 1 ] sp "SELECT k FROM kv WHERE v = ?" >>= Dataframe.collect >>= fun rows ->
      check_rows "positional param" [ "[a]" ] rows;
      Session.Catalog.table_exists sp "kv" >>= fun ex ->
      Alcotest.(check bool) "temp view exists" true ex;
      Dataframe.collect (Dataframe.select_names (Session.Catalog.list_tables sp) [ "name"; "isTemporary" ])
      >>= fun rows ->
      Alcotest.(check bool) "list_tables has kv" true (List.mem "[kv, true]" (strs rows));
      Session.Catalog.drop_temp_view sp "kv" >>= fun dropped ->
      Alcotest.(check bool) "dropped" true dropped;
      Session.sql sp "CREATE OR REPLACE TEMP VIEW t2 AS SELECT 7 AS seven" >>= fun _ ->
      Session.sql sp "SELECT seven * 6 FROM t2" >>= Dataframe.collect >|= fun rows ->
      check_rows "ddl then query" [ "[42]" ] rows)

let test_write_read () =
  run (fun () ->
      let sp = s () in
      let path = "/tmp/spark-connect-ocaml-test/parquet" in
      let df =
        Dataframe.with_column (Session.range sp 10L) "even" Column.O.(Column.col "id" % Column.int 2 = Column.int 0)
      in
      Dataframe.write df |> fun w ->
      Dataframe.Writer.mode w `Overwrite |> fun w ->
      Dataframe.Writer.partition_by w [ "even" ] |> fun w ->
      Dataframe.Writer.parquet w path >>= fun () ->
      let back = Session.read sp |> fun r -> Session.Reader.parquet r [ path ] in
      (* partition values come back as strings (Hive-style path inference) *)
      Dataframe.count (Dataframe.filter back Column.O.(Column.col "even" = Column.string "true")) >>= fun n ->
      Alcotest.(check int64) "read back partition" 5L n;
      Dataframe.input_files back >>= fun files ->
      Alcotest.(check bool) "input files" true (List.length files >= 2);
      Dataframe.write df |> fun w ->
      Dataframe.Writer.mode w `Overwrite |> fun w ->
      Dataframe.Writer.save_as_table w "sc_ocaml_tbl" >>= fun () ->
      Dataframe.count (Session.table sp "sc_ocaml_tbl") >>= fun n ->
      Alcotest.(check int64) "table" 10L n;
      Session.sql sp "DROP TABLE sc_ocaml_tbl" >|= fun _ -> ())

let test_analysis () =
  run (fun () ->
      let sp = s () in
      let df =
        Dataframe.select (Session.range sp 5L)
          [ Column.alias (Column.cast (Column.col "id") "string") "s"; Column.col "id" ]
      in
      Dataframe.schema df >>= fun sc ->
      Alcotest.(check string) "schema" "struct<s:string,id:bigint>" (Datatype.simple_string sc);
      Dataframe.dtypes df >>= fun d ->
      Alcotest.(check (list (pair string string))) "dtypes" [ ("s", "string"); ("id", "bigint") ] d;
      Dataframe.tree_string df >>= fun t ->
      Alcotest.(check bool) "tree" true (String.length t > 0 && String.sub t 0 4 = "root");
      Dataframe.explain_string ~mode:`Extended df >>= fun e ->
      Alcotest.(check bool)
        "explain" true
        (try
           ignore (Str.search_forward (Str.regexp_string "Physical Plan") e 0);
           true
         with Not_found -> false);
      Dataframe.is_local df >>= fun l ->
      Alcotest.(check bool) "is_local" false l;
      Dataframe.same_semantics df df >>= fun same ->
      Alcotest.(check bool) "same semantics" true same;
      Dataframe.persist df >>= fun df ->
      Dataframe.count df >>= fun _ ->
      Dataframe.unpersist df >>= fun _ ->
      Dataframe.show_string ~n:2 ~vertical:true df >|= fun s ->
      Alcotest.(check bool) "vertical show" true (String.length s > 0 && s.[0] = '-'))

let test_functions () =
  run (fun () ->
      let sp = s () in
      let df =
        Session.create_dataframe sp ~columns:[ "s"; "n" ]
          [ [ Value.String "hello"; Value.Int 3 ]; [ Value.String "Bye"; Value.Null ] ]
      in
      let out =
        Dataframe.select df
          [
            Column.O.(F.upper (Column.col "s") @= "u");
            Column.alias (F.coalesce [ Column.col "n"; Column.int (-1) ]) "n2";
            Column.alias (F.concat_ws "-" [ Column.col "s"; Column.cast (Column.col "n") "string" ]) "cw";
            Column.alias
              (Column.otherwise
                 (Column.when_ (Column.is_null (Column.col "n")) (Column.string "none"))
                 (Column.string "some"))
              "w";
            Column.alias (F.array [ Column.int 1; Column.int 2 ]) "arr";
            Column.alias
              (F.transform (F.array [ Column.int 1; Column.int 2 ]) (fun x -> Column.O.(x * Column.int 10)))
              "tr";
            Column.alias (Column.get_field (F.struct_ [ Column.alias (Column.col "n") "x" ]) "x") "gf";
            Column.alias (F.length (Column.col "s")) "len";
          ]
      in
      Dataframe.collect out >>= fun rows ->
      check_rows "functions"
        [ "[HELLO, 3, hello-3, some, [1, 2], [10, 20], 3, 5]"; "[BYE, -1, Bye, none, [1, 2], [10, 20], NULL, 3]" ]
        rows;
      (* window *)
      let w =
        Column.Window.partition_by [ Column.col "g" ] |> fun w -> Column.Window.with_order_by w [ Column.col "id" ]
      in
      let df = Dataframe.with_column (Session.range sp 6L) "g" Column.O.(Column.col "id" % Column.int 2) in
      let df = Dataframe.with_column df "rn" (Column.over (F.row_number ()) w) in
      Dataframe.collect (Dataframe.sort (Dataframe.select_names df [ "id"; "rn" ]) [ Column.col "id" ]) >|= fun rows ->
      check_rows "window" [ "[0, 1]"; "[1, 1]"; "[2, 2]"; "[3, 2]"; "[4, 3]"; "[5, 3]" ] rows)

let test_conf () =
  run (fun () ->
      let sp = s () in
      Session.Conf.set sp "spark.sql.shuffle.partitions" "7" >>= fun () ->
      Session.Conf.get sp "spark.sql.shuffle.partitions" >>= fun v ->
      Alcotest.(check string) "get" "7" v;
      Session.Conf.get_opt sp "spark.sql.no.such.key" >>= fun v ->
      Alcotest.(check (option string)) "get_opt" None v;
      Session.Conf.get_with_default sp "spark.sql.no.such.key" "dflt" >>= fun v ->
      Alcotest.(check string) "default" "dflt" v;
      Session.Conf.is_modifiable sp "spark.sql.shuffle.partitions" >>= fun m ->
      Alcotest.(check bool) "modifiable" true m;
      Session.Conf.get_all ~prefix:"spark.sql.shuffle" sp >>= fun all ->
      Alcotest.(check bool) "get_all strips prefix" true (List.mem_assoc ".partitions" all);
      Session.Conf.unset sp "spark.sql.shuffle.partitions" >>= fun () ->
      Session.Catalog.current_database sp >|= fun db -> Alcotest.(check string) "db" "default" db)

let test_errors () =
  run (fun () ->
      let sp = s () in
      Lwt.catch
        (fun () ->
          Session.sql sp "SELECT * FROM no_such_table_xyz" >>= Dataframe.collect >|= fun _ ->
          Alcotest.fail "expected error")
        (function
          | Spark_error e -> (
              Alcotest.(check (option string)) "error class" (Some "TABLE_OR_VIEW_NOT_FOUND") e.error_class;
              Alcotest.(check (option string)) "sqlstate" (Some "42P01") e.sql_state;
              Alcotest.(check bool)
                "exception classes" true
                (List.mem "org.apache.spark.sql.AnalysisException" e.exception_classes);
              Session.fetch_error_details sp e >|= fun details ->
              match details with
              | Some d -> Alcotest.(check bool) "details have errors" true (List.length d.errors > 0)
              | None -> Alcotest.fail "no error id")
          | e -> Lwt.fail e)
      >>= fun () ->
      Lwt.catch
        (fun () ->
          Dataframe.collect (Dataframe.select (Session.range sp 1L) [ Column.col "nope" ]) >|= fun _ ->
          Alcotest.fail "expected error")
        (function
          | Spark_error e ->
              Lwt.return
                (Alcotest.(check (option string)) "unresolved" (Some "UNRESOLVED_COLUMN.WITH_SUGGESTION") e.error_class)
          | e -> Lwt.fail e))

let test_large () =
  run (fun () ->
      let sp = s () in
      Dataframe.collect_columns
        (Dataframe.select (Session.range sp 1_000_000L)
           [ Column.col "id"; Column.alias (Column.cast (Column.col "id") "string") "s" ])
      >|= fun (names, cols) ->
      Alcotest.(check (list string)) "names" [ "id"; "s" ] names;
      Alcotest.(check int) "rows" 1_000_000 (Array.length (List.hd cols));
      Alcotest.(check string) "last" "999999" (Value.to_string (List.nth cols 1).(999_999)))

let () =
  match remote with
  | None -> print_endline "SPARK_REMOTE not set; skipping Spark integration tests"
  | Some _ ->
      Alcotest.run "spark"
        [
          ( "session",
            [
              Alcotest.test_case "version" `Quick test_version;
              Alcotest.test_case "conf and catalog" `Quick test_conf;
              Alcotest.test_case "sql params and views" `Quick test_sql_params_views;
              Alcotest.test_case "errors" `Quick test_errors;
            ] );
          ( "dataframe",
            [
              Alcotest.test_case "sql types" `Quick test_sql_types;
              Alcotest.test_case "transforms" `Quick test_dataframe_ops;
              Alcotest.test_case "joins" `Quick test_join;
              Alcotest.test_case "functions and windows" `Quick test_functions;
              Alcotest.test_case "analysis" `Quick test_analysis;
              Alcotest.test_case "write and read" `Quick test_write_read;
              Alcotest.test_case "large result" `Slow test_large;
            ] );
        ]
