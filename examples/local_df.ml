open Lwt.Infix
open Spark_connect

let url = try Sys.getenv "SPARK_REMOTE" with Not_found -> "sc://localhost:15002"

let () =
  (* dump an IPC stream for external validation *)
  let data =
    Arrow_writer.encode
      ~columns:[ "id"; "name"; "score"; "ok"; "d"; "ts"; "dec"; "tags"; "pt" ]
      [
        [
          Value.Int 1;
          Value.String "ann";
          Value.Float 1.5;
          Value.Bool true;
          Value.Date 19000;
          Value.Timestamp (1_700_000_000_000_000L, Value.Microsecond, Some "UTC");
          Value.Decimal "12.34";
          Value.List [| Value.String "a"; Value.String "b" |];
          Value.Struct [| ("x", Value.Int 1); ("y", Value.Float 2.0) |];
        ];
        [
          Value.Int 2;
          Value.Null;
          Value.Float (-2.25);
          Value.Null;
          Value.Null;
          Value.Null;
          Value.Decimal "-0.01";
          Value.List [||];
          Value.Null;
        ];
        [
          Value.Int64 3L;
          Value.String "cé";
          Value.Null;
          Value.Bool false;
          Value.Date 0;
          Value.Timestamp (0L, Value.Microsecond, Some "UTC");
          Value.Null;
          Value.Null;
          Value.Struct [| ("x", Value.Null); ("y", Value.Float 3.5) |];
        ];
      ]
  in
  let oc = open_out_bin Sys.argv.(1) in
  output_string oc data;
  close_out oc;
  (* read it back with our own reader *)
  let schema, batches = Arrow.read_stream data in
  print_endline (Arrow.schema_to_string (Option.get schema));
  List.iter
    (fun b ->
      List.iter
        (fun r -> print_endline (String.concat " | " (Array.to_list (Array.map Value.to_string r))))
        (Arrow.rows b))
    batches;
  Lwt_main.run
    ( Session.connect url >>= fun spark ->
      let df =
        Session.create_dataframe spark
          ~columns:[ "id"; "name"; "score"; "ok"; "d"; "ts"; "dec"; "tags"; "pt" ]
          [
            [
              Value.Int 1;
              Value.String "ann";
              Value.Float 1.5;
              Value.Bool true;
              Value.Date 19000;
              Value.Timestamp (1_700_000_000_000_000L, Value.Microsecond, Some "UTC");
              Value.Decimal "12.34";
              Value.List [| Value.String "a"; Value.String "b" |];
              Value.Struct [| ("x", Value.Int 1); ("y", Value.Float 2.0) |];
            ];
            [
              Value.Int 2;
              Value.Null;
              Value.Float (-2.25);
              Value.Null;
              Value.Null;
              Value.Null;
              Value.Decimal "-0.01";
              Value.List [||];
              Value.Null;
            ];
          ]
      in
      Dataframe.print_schema df >>= fun () ->
      Dataframe.show ~truncate:0 df >>= fun () ->
      Dataframe.collect df >>= fun rows ->
      List.iter (fun r -> print_endline (Row.to_string r)) rows;
      (* multi-batch result *)
      let big = Session.range spark 3_000_000L in
      let t0 = Unix.gettimeofday () in
      Dataframe.collect_columns
        (Dataframe.select big
           [ Column.col "id"; Column.alias (F.call "sha1" [ Column.cast (Column.col "id") "string" ]) "h" ])
      >>= fun (names, cols) ->
      Printf.printf "big: %s rows=%d in %.2fs\n%!" (String.concat "," names)
        (Array.length (List.hd cols))
        (Unix.gettimeofday () -. t0);
      Session.close spark )
