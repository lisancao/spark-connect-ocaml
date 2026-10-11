(* Connect, build a DataFrame, aggregate it, and print the result.

     SPARK_REMOTE=sc://localhost:15002 dune exec examples/quickstart.exe *)

open Lwt.Infix
open Spark_connect

let url = try Sys.getenv "SPARK_REMOTE" with Not_found -> "sc://localhost:15002"

let () =
  Lwt_main.run
    ( Session.connect url >>= fun spark ->
      let open Column.O in
      let df =
        Session.range ~start:1L spark 1000L |> fun d ->
        Dataframe.select d [ Column.col "id"; (Column.col "id" % Column.int 3) @= "bucket" ] |> fun d ->
        Dataframe.group_by d [ Column.col "bucket" ] |> fun g ->
        Dataframe.Grouped.agg g [ F.count_all () @= "n"; F.sum (Column.col "id") @= "total" ] |> fun d ->
        Dataframe.sort d [ Column.col "bucket" ]
      in
      Dataframe.show df >>= fun () -> Session.close spark )
