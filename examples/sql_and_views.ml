(* Mix the DataFrame API and SQL through temporary views, with named and positional
   parameters.

     SPARK_REMOTE=sc://localhost:15002 dune exec examples/sql_and_views.exe *)

open Lwt.Infix
open Spark_connect

let url = try Sys.getenv "SPARK_REMOTE" with Not_found -> "sc://localhost:15002"

let () =
  Lwt_main.run
    ( Session.connect url >>= fun spark ->
      let people =
        Session.create_dataframe spark ~columns:[ "name"; "dept"; "salary" ]
          [
            [ Value.String "ann"; Value.String "eng"; Value.Int 120 ];
            [ Value.String "bob"; Value.String "eng"; Value.Int 95 ];
            [ Value.String "cy"; Value.String "sales"; Value.Int 80 ];
          ]
      in
      Dataframe.create_or_replace_temp_view people "people" >>= fun () ->
      Session.sql spark
        ~args:[ ("dept", Value.String "eng") ]
        "SELECT name, salary FROM people WHERE dept = :dept ORDER BY salary DESC"
      >>= Dataframe.show
      >>= fun () ->
      Session.sql spark ~pos_args:[ Value.Int 90 ] "SELECT count(*) AS above FROM people WHERE salary > ?"
      >>= Dataframe.show
      >>= fun () ->
      (* SQL results are DataFrames, so the DataFrame API keeps going from here *)
      Session.sql spark "SELECT dept, avg(salary) AS avg_salary FROM people GROUP BY dept" >>= fun by_dept ->
      Dataframe.show (Dataframe.sort by_dept [ Column.col "dept" ]) >>= fun () -> Session.close spark )
