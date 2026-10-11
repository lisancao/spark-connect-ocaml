(* Split lines into words, explode, and count.

     SPARK_REMOTE=sc://localhost:15002 dune exec examples/word_count.exe *)

open Lwt.Infix
open Spark_connect

let url = try Sys.getenv "SPARK_REMOTE" with Not_found -> "sc://localhost:15002"
let col = Column.col

let () =
  Lwt_main.run
    ( Session.connect url >>= fun spark ->
      let lines =
        Session.create_dataframe spark ~columns:[ "line" ]
          [
            [ Value.String "the quick brown fox" ]; [ Value.String "the lazy dog" ]; [ Value.String "The fox, the dog" ];
          ]
      in
      let words =
        Dataframe.select lines
          [ Column.alias (F.explode (F.split (F.lower (F.regexp_replace (col "line") "[^a-zA-Z ]" "")) " ")) "word" ]
      in
      Dataframe.Grouped.count (Dataframe.group_by words [ col "word" ]) |> fun d ->
      Dataframe.sort d [ Column.desc (col "count"); col "word" ] |> Dataframe.show ~n:5 >>= fun () ->
      Session.close spark )
