(* Write a DataFrame as partitioned Parquet on the server's filesystem, read it back,
   and save it as a table.

     SPARK_REMOTE=sc://localhost:15002 dune exec examples/read_write.exe *)

open Lwt.Infix
open Spark_connect

let url = try Sys.getenv "SPARK_REMOTE" with Not_found -> "sc://localhost:15002"
let path = "/tmp/spark-connect-ocaml-example/events"

let () =
  Lwt_main.run
    ( Session.connect url >>= fun spark ->
      let events =
        Dataframe.with_columns (Session.range spark 100L)
          [
            ( "kind",
              Column.O.(
                Column.otherwise
                  (Column.when_ (Column.col "id" % Column.int 2 = Column.int 0) (Column.string "even"))
                  (Column.string "odd")) );
          ]
      in
      Dataframe.write events |> fun w ->
      Dataframe.Writer.mode w `Overwrite |> fun w ->
      Dataframe.Writer.partition_by w [ "kind" ] |> fun w ->
      Dataframe.Writer.parquet w path >>= fun () ->
      let back = Session.Reader.parquet (Session.read spark) [ path ] in
      Dataframe.count back >>= fun n ->
      Printf.printf "read back %Ld rows from %s\n%!" n path;
      Dataframe.Grouped.count (Dataframe.group_by back [ Column.col "kind" ]) |> fun d ->
      Dataframe.show (Dataframe.sort d [ Column.col "kind" ]) >>= fun () ->
      Dataframe.write back |> fun w ->
      Dataframe.Writer.mode w `Overwrite |> fun w ->
      Dataframe.Writer.save_as_table w "example_events" >>= fun () ->
      Session.Catalog.table_exists spark "example_events" >>= fun exists ->
      Printf.printf "table example_events exists: %b\n%!" exists;
      Session.sql spark "DROP TABLE example_events" >>= fun _ -> Session.close spark )
