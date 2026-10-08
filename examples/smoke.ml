open Lwt.Infix
open Spark_connect

let url = try Sys.getenv "SPARK_REMOTE" with Not_found -> "sc://localhost:15002"

let () =
  Lwt_main.run
    ( Session.connect url >>= fun spark ->
      Session.version spark >>= fun v ->
      Printf.printf "server version: %s\n%!" v;
      Session.sql spark "SELECT id, id * 2 AS twice, CAST(id AS STRING) AS s, id % 2 = 0 AS even FROM range(5)"
      >>= fun df ->
      Dataframe.show df >>= fun () ->
      Dataframe.collect df >>= fun rows ->
      List.iter (fun r -> print_endline (Row.to_string r)) rows;
      Dataframe.print_schema df >>= fun () ->
      let df2 =
        Session.range spark 10L |> fun d ->
        Dataframe.filter d Column.O.(Column.col "id" > Column.int 6) |> fun d ->
        Dataframe.with_column d "sq" Column.O.(Column.col "id" * Column.col "id") |> fun d ->
        Dataframe.select d [ Column.col "id"; Column.col "sq"; Column.alias (F.upper (Column.string "ok")) "u" ]
      in
      Dataframe.show df2 >>= fun () ->
      Dataframe.count df2 >>= fun n ->
      Printf.printf "count = %Ld\n%!" n;
      Dataframe.dtypes df2 >>= fun dts ->
      List.iter (fun (n, t) -> Printf.printf "  %s: %s\n" n t) dts;
      Lwt.catch
        (fun () ->
          Session.sql spark "SELECT 1/0 AS x, assert_true(false)" >>= fun d ->
          Dataframe.collect d >|= fun _ -> ())
        (function
          | Spark_error e ->
              Printf.printf "expected error: %s\n  error_class=%s sqlState=%s\n%!" (Error.to_string e)
                (Option.value ~default:"-" e.error_class)
                (Option.value ~default:"-" e.sql_state);
              Lwt.return_unit
          | e -> Lwt.fail e)
      >>= fun () -> Session.close spark )
