(* Window functions: rank within a partition and a running total.

     SPARK_REMOTE=sc://localhost:15002 dune exec examples/window_functions.exe *)

open Lwt.Infix
open Spark_connect

let url = try Sys.getenv "SPARK_REMOTE" with Not_found -> "sc://localhost:15002"
let col = Column.col

let () =
  Lwt_main.run
    ( Session.connect url >>= fun spark ->
      let sales =
        Session.create_dataframe spark ~columns:[ "region"; "month"; "amount" ]
          [
            [ Value.String "east"; Value.Int 1; Value.Int 100 ];
            [ Value.String "east"; Value.Int 2; Value.Int 140 ];
            [ Value.String "east"; Value.Int 3; Value.Int 90 ];
            [ Value.String "west"; Value.Int 1; Value.Int 70 ];
            [ Value.String "west"; Value.Int 2; Value.Int 120 ];
          ]
      in
      let by_amount =
        Column.Window.partition_by [ col "region" ] |> fun w ->
        Column.Window.with_order_by w [ Column.desc (col "amount") ]
      in
      let running =
        Column.Window.partition_by [ col "region" ] |> fun w ->
        Column.Window.with_order_by w [ col "month" ] |> fun w ->
        Column.Window.rows_between w Column.Window.unbounded_preceding Column.Window.current_row
      in
      Dataframe.with_columns sales
        [ ("rank", Column.over (F.rank ()) by_amount); ("running_total", Column.over (F.sum (col "amount")) running) ]
      |> (fun d -> Dataframe.sort d [ col "region"; col "month" ])
      |> Dataframe.show
      >>= fun () -> Session.close spark )
