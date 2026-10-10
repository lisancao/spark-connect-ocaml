(* An end-to-end batch job over a month of NYC yellow-cab trips: read Parquet and CSV,
   clean, derive columns, join, aggregate, rank with a window function, write Parquet,
   and read the results back.

     TAXI_DATA=/data SPARK_REMOTE=sc://localhost:15002 dune exec examples/nyc_taxi.exe

   TAXI_DATA is the directory as the *server* sees it (it must contain
   yellow_tripdata_2024-01.parquet and taxi_zone_lookup.csv); results go to
   $TAXI_DATA/out. examples/nyc_taxi/README.md has a Docker cluster to run it on. *)

open Lwt.Infix
open Spark_connect

let url = try Sys.getenv "SPARK_REMOTE" with Not_found -> "sc://localhost:15002"
let data = try Sys.getenv "TAXI_DATA" with Not_found -> "/data"
let col = Column.col

let step name f =
  let t0 = Unix.gettimeofday () in
  f () >|= fun x ->
  Printf.printf "  %-36s %6.2fs\n%!" name (Unix.gettimeofday () -. t0);
  x

let () =
  Lwt_main.run
    ( Session.connect url >>= fun spark ->
      Session.version spark >>= fun v ->
      Printf.printf "Spark %s at %s\n\n%!" v url;
      Session.Conf.set spark "spark.sql.session.timeZone" "UTC" >>= fun () ->
      let read = Session.read spark in
      let trips = Session.Reader.parquet read [ data ^ "/yellow_tripdata_2024-01.parquet" ] in
      let zones =
        Session.Reader.option read "header" "true" |> fun r ->
        Session.Reader.option r "inferSchema" "true" |> fun r -> Session.Reader.csv r [ data ^ "/taxi_zone_lookup.csv" ]
      in

      (* clean: positive distance and fare, pickups inside January, plausible durations *)
      let duration_min =
        Column.O.(
          (F.unix_timestamp (col "tpep_dropoff_datetime") - F.unix_timestamp (col "tpep_pickup_datetime"))
          / Column.int 60)
      in
      let clean =
        Dataframe.with_columns trips
          [
            ("duration_min", duration_min);
            ("pickup_date", F.to_date (col "tpep_pickup_datetime"));
            ("tip_pct", Column.O.(col "tip_amount" / col "fare_amount" * Column.int 100));
          ]
        |> fun d ->
        Dataframe.filter d
          Column.O.(
            col "trip_distance" > Column.int 0
            && col "fare_amount" > Column.int 0
            && col "tpep_pickup_datetime" >= Column.string "2024-01-01"
            && col "tpep_pickup_datetime" < Column.string "2024-02-01"
            && Column.between (col "duration_min") ~low:(Column.int 1) ~high:(Column.int 240))
      in

      (* enrich with the pickup borough and zone (the lookup is tiny, so broadcast it) *)
      let pickup_zones =
        Dataframe.hint zones "broadcast" [] |> fun z ->
        Dataframe.select z [ Column.alias (col "LocationID") "pu_id"; col "Borough"; col "Zone" ]
      in
      let enriched = Dataframe.join clean pickup_zones ~on:Column.O.(col "PULocationID" = col "pu_id") in

      Printf.printf "steps\n%!";
      step "count raw trips" (fun () -> Dataframe.count trips) >>= fun raw ->
      step "count clean trips" (fun () -> Dataframe.count clean) >>= fun kept ->
      (* per-zone metrics, then the top three zones per borough by trips *)
      let per_zone =
        Dataframe.group_by enriched [ col "Borough"; col "Zone" ] |> fun g ->
        Dataframe.Grouped.agg g
          Column.O.
            [
              F.count_all () @= "trips";
              F.round ~scale:2 (F.avg (col "fare_amount")) @= "avg_fare";
              F.round ~scale:1 (F.avg (col "tip_pct")) @= "avg_tip_pct";
              F.round ~scale:1 (F.avg (col "duration_min")) @= "avg_minutes";
              Column.cast (F.round (F.sum (col "total_amount"))) "bigint" @= "revenue";
            ]
      in
      let by_borough =
        Column.Window.partition_by [ col "Borough" ] |> fun w ->
        Column.Window.with_order_by w [ Column.desc (col "trips") ]
      in
      let top_zones =
        Dataframe.with_column per_zone "rank" (Column.over (F.rank ()) by_borough) |> fun d ->
        Dataframe.filter d Column.O.(col "rank" <= Column.int 3 && not (Column.is_null (col "Borough"))) |> fun d ->
        Dataframe.sort d [ col "Borough"; col "rank" ]
      in
      let daily =
        Dataframe.group_by enriched [ col "pickup_date" ] |> fun g ->
        Dataframe.Grouped.agg g
          Column.O.
            [ F.count_all () @= "trips"; Column.cast (F.round (F.sum (col "total_amount"))) "bigint" @= "revenue" ]
        |> fun d -> Dataframe.sort d [ col "pickup_date" ]
      in

      let out = data ^ "/out" in
      let write df path =
        Dataframe.write df |> fun w ->
        Dataframe.Writer.mode w `Overwrite |> fun w -> Dataframe.Writer.parquet w path
      in
      step "write top_zones (parquet)" (fun () -> write top_zones (out ^ "/top_zones")) >>= fun () ->
      step "write daily (parquet)" (fun () -> write daily (out ^ "/daily")) >>= fun () ->
      (* read the written results back, so the output is what landed on disk *)
      let back name = Session.Reader.parquet (Session.read spark) [ out ^ "/" ^ name ] in
      step "read results back" (fun () ->
          Dataframe.count (back "top_zones") >>= fun z ->
          Dataframe.count (back "daily") >|= fun d -> (z, d))
      >>= fun (nz, nd) ->
      Printf.printf "\ntrips: %Ld raw, %Ld kept after cleaning (%.1f%% dropped)\n" raw kept
        (100. *. (1. -. (Int64.to_float kept /. Int64.to_float raw)));
      Printf.printf "wrote %Ld top-zone rows and %Ld daily rows to %s\n\n%!" nz nd out;
      print_endline "Top 3 pickup zones per borough";
      Dataframe.show ~n:30 ~truncate:28 (Dataframe.sort (back "top_zones") [ col "Borough"; col "rank" ]) >>= fun () ->
      print_endline "Busiest and quietest days";
      Dataframe.show ~n:3 (Dataframe.sort (back "daily") [ Column.desc (col "trips") ]) >>= fun () ->
      Dataframe.show ~n:3 (Dataframe.sort (back "daily") [ col "trips" ]) >>= fun () -> Session.close spark )
