(* A small options desk on Spark Connect.

   Market history comes in through a Python data source (the FRED adapter in
   pyspark_fred/), lands in Delta, and Spark computes returns, realized volatility and
   drawdowns over ten years of index levels. The desk's OCaml pricing code (Case
   Ollila's option_pricer, vendored in option_pricer/) prices 30-day options from those
   numbers, the priced book goes back to Spark as a Delta table, a metric view turns it
   into risk measures, and the next day's fills are merged with the previous book still
   reachable by time travel.

     DESK_SINCE=2016-10-10 SPARK_REMOTE=sc://localhost:15002 dune exec examples/options_desk/options_desk.exe

   Every step that can be cross-checked prints PASS or FAIL. *)

open Lwt.Infix
open Spark_connect
module BS = Option_pricer.Black_scholes
module T = Option_pricer.Types

let url = try Sys.getenv "SPARK_REMOTE" with Not_found -> "sc://localhost:15002"
let since = try Sys.getenv "DESK_SINCE" with Not_found -> "2016-10-10"
let col = Column.col
let indices = [ "SP500"; "NASDAQCOM"; "DJIA" ]
let failures = ref 0

let check name ok detail =
  if not ok then incr failures;
  Printf.printf "  %s  %s%s\n%!" (if ok then "PASS" else "FAIL") name (if detail = "" then "" else ": " ^ detail)

let header s = Printf.printf "\n== %s\n%!" s

let fred spark series =
  Session.read spark |> fun r ->
  Session.Reader.format r "fred" |> fun r ->
  Session.Reader.option r "series" (String.concat "," series) |> fun r -> Session.Reader.load r

let write_delta df table =
  let w = Dataframe.write_to df table in
  Dataframe.Writer_v2.using w "delta" |> Dataframe.Writer_v2.create_or_replace

let f = Value.to_float

(* the desk's positions: contracts per (index, kind); each contract is 100 x the index *)
let positions = [ ("SP500", T.Call, 20); ("SP500", T.Put, -10); ("NASDAQCOM", T.Call, 5); ("DJIA", T.Put, 8) ]

let () =
  Lwt_main.run
    ( Session.connect url >>= fun spark ->
      Session.version spark >>= fun v ->
      Printf.printf "Spark %s at %s\n%!" v url;
      Session.Conf.set spark "spark.sql.session.timeZone" "UTC" >>= fun () ->
      (* ---------------------------------------------------------------- 1. ingest *)
      header "1. Ingest: FRED through the Python data source, into Delta";
      let prices = Dataframe.filter (fred spark indices) Column.O.(col "date" >= Column.string since) in
      write_delta prices "desk_prices" >>= fun () ->
      Session.sql spark "SELECT series, count(*), min(date), max(date) FROM desk_prices GROUP BY series ORDER BY series"
      >>= Dataframe.show
      >>= fun () ->
      (* ------------------------------------------------------------- 2. analytics *)
      header "2. Analytics in Spark: returns, realized volatility, drawdowns";
      let by_day =
        Column.Window.partition_by [ col "series" ] |> fun w -> Column.Window.with_order_by w [ col "date" ]
      in
      let last_21 =
        Column.Window.rows_between by_day (Column.Window.Value (Column.int (-20))) Column.Window.current_row
      in
      let to_date = Column.Window.rows_between by_day Column.Window.unbounded_preceding Column.Window.current_row in
      let daily =
        Dataframe.with_columns (Session.table spark "desk_prices")
          [ ("ret", F.log Column.O.(col "value" / Column.over (F.lag (col "value")) by_day)) ]
        |> fun d ->
        Dataframe.with_columns d
          [
            ("rv21", Column.O.(Column.over (F.stddev (col "ret")) last_21 * F.sqrt (Column.int 252)));
            ("drawdown", Column.O.((col "value" / Column.over (F.max (col "value")) to_date) - Column.int 1));
          ]
      in
      write_delta daily "desk_daily" >>= fun () ->
      let latest =
        Column.Window.partition_by [ col "series" ] |> fun w ->
        Column.Window.with_order_by w [ Column.desc (col "date") ]
      in
      let snapshot =
        Dataframe.with_column (Session.table spark "desk_daily") "rn" (Column.over (F.row_number ()) latest) |> fun d ->
        Dataframe.filter d Column.O.(col "rn" = Column.int 1)
      in
      let history =
        Dataframe.group_by (Session.table spark "desk_daily") [ col "series" ] |> fun g ->
        Dataframe.Grouped.agg g
          Column.O.
            [
              F.round ~scale:1 (F.min (col "drawdown") * Column.int 100) @= "max_drawdown_pct";
              F.round ~scale:1 (F.min (col "ret") * Column.int 100) @= "worst_day_pct";
              F.round ~scale:1 (F.avg (col "rv21") * Column.int 100) @= "avg_rv21_pct";
            ]
      in
      let report =
        Dataframe.join_using snapshot history [ "series" ] |> fun d ->
        Dataframe.select d
          Column.O.
            [
              col "series";
              col "date";
              F.round ~scale:2 (col "value") @= "spot";
              F.round ~scale:1 (col "rv21" * Column.int 100) @= "rv21_pct";
              col "avg_rv21_pct";
              col "max_drawdown_pct";
              col "worst_day_pct";
            ]
        |> fun d -> Dataframe.sort d [ col "series" ]
      in
      Dataframe.show report >>= fun () ->
      (* implied (VIX) against realized for the S&P 500, day by day *)
      let vix =
        Dataframe.select (fred spark [ "VIXCLS" ]) Column.O.[ col "date"; (col "value" / Column.int 100) @= "implied" ]
      in
      let spx = Dataframe.filter (Session.table spark "desk_daily") Column.O.(col "series" = Column.string "SP500") in
      let vs = Dataframe.join_using (Dataframe.filter spx (Column.is_not_null (col "rv21"))) vix [ "date" ] in
      Dataframe.agg vs
        Column.O.
          [
            F.count_all () @= "days";
            F.round ~scale:1 (F.avg (col "implied" - col "rv21") * Column.int 100) @= "avg_implied_minus_realized_pts";
            F.round ~scale:1 (F.avg (Column.cast (col "implied" > col "rv21") "int") * Column.int 100)
            @= "pct_days_implied_above";
          ]
      |> Dataframe.show
      >>= fun () ->
      (* ---------------------------------------------------------------- 3. pricing *)
      header "3. Pricing in OCaml: 30-day options with option_pricer's Black-Scholes";
      Dataframe.collect (Dataframe.select report [ col "series"; col "spot"; col "rv21_pct" ]) >>= fun snap ->
      Dataframe.collect
        (Dataframe.sort
           (Dataframe.filter (fred spark [ "VIXCLS"; "DGS3MO" ]) (Column.is_not_null (col "value")))
           [ Column.desc (col "date") ])
      >>= fun latest_rates ->
      let latest_of s =
        f (Row.field (List.find (fun r -> Value.to_str (Row.field r "series") = s) latest_rates) "value")
      in
      let rate = latest_of "DGS3MO" /. 100. and vix_now = latest_of "VIXCLS" /. 100. in
      Printf.printf "  risk-free (3m T-bill) %.2f%%, VIX %.1f\n" (rate *. 100.) (vix_now *. 100.);
      let market_for series ~vol =
        let r = List.find (fun r -> Value.to_str (Row.field r "series") = series) snap in
        let spot = f (Row.field r "spot") in
        { T.spot; strike = spot; ttm = 30. /. 365.; rate; div_yield = 0.0; vol }
      in
      let rv series =
        f (Row.field (List.find (fun r -> Value.to_str (Row.field r "series") = series) snap) "rv21_pct") /. 100.
      in
      let book =
        List.map
          (fun (series, kind, contracts) ->
            let m = market_for series ~vol:(rv series) in
            let g = BS.greeks kind m in
            ( series,
              kind,
              contracts,
              m,
              BS.price kind m,
              g.delta,
              g.gamma,
              BS.vega_per_point g.vega,
              BS.theta_per_day g.theta ))
          positions
      in
      Printf.printf "  %-10s %-4s %9s %10s %6s %8s %7s %9s %9s\n" "index" "kind" "contracts" "strike" "vol" "premium"
        "delta" "vega/pt" "theta/day";
      List.iter
        (fun (s, k, n, (m : T.market), p, d, _, vg, th) ->
          Printf.printf "  %-10s %-4s %9d %10.2f %5.1f%% %8.2f %7.3f %9.2f %9.2f\n" s (T.string_of_option_type k) n
            m.strike (m.vol *. 100.) p d vg th)
        book;
      List.iter
        (fun series ->
          let m = market_for series ~vol:(rv series) in
          let gap = BS.parity_gap m in
          check (Printf.sprintf "put-call parity, %s" series) (Float.abs gap < 1e-8) (Printf.sprintf "gap %.2e" gap))
        indices;
      let m_rv = market_for "SP500" ~vol:(rv "SP500") and m_iv = market_for "SP500" ~vol:vix_now in
      Printf.printf "  SP500 30-day ATM call: %.2f at realized vol (%.1f%%), %.2f at VIX (%.1f%%)\n"
        (BS.price T.Call m_rv) (m_rv.vol *. 100.) (BS.price T.Call m_iv) (m_iv.vol *. 100.);
      let rows =
        List.map
          (fun (s, k, n, (m : T.market), p, d, g, vg, th) ->
            [
              Value.String s;
              Value.String (T.string_of_option_type k);
              Value.Int n;
              Value.Float m.spot;
              Value.Float m.strike;
              Value.Float m.vol;
              Value.Float p;
              Value.Float d;
              Value.Float g;
              Value.Float vg;
              Value.Float th;
            ])
          book
      in
      let book_df =
        Session.create_dataframe_ddl spark
          ~ddl:
            "index_name string, kind string, contracts int, spot double, strike double, vol double, premium double, \
             delta double, gamma double, vega_pt double, theta_day double"
          ~columns:
            [
              "index_name";
              "kind";
              "contracts";
              "spot";
              "strike";
              "vol";
              "premium";
              "delta";
              "gamma";
              "vega_pt";
              "theta_day";
            ]
          rows
      in
      write_delta book_df "desk_book" >>= fun () ->
      Dataframe.count (Session.table spark "desk_book") >>= fun n ->
      check "priced book written to Delta" (n = Int64.of_int (List.length positions)) (Printf.sprintf "%Ld positions" n);
      (* -------------------------------------------------------------- 4. risk layer *)
      header "4. Risk layer: a metric view over the book";
      (* Drop first: in the session catalog, Spark 4.2 and 4.3.0-rc1 accept CREATE OR REPLACE
         for a metric view but ignore OR REPLACE, failing with TABLE_OR_VIEW_ALREADY_EXISTS. *)
      Session.sql spark "DROP VIEW IF EXISTS desk_risk" >>= fun _ ->
      Session.sql spark
        {|CREATE VIEW desk_risk WITH METRICS LANGUAGE YAML AS $$
version: "0.1"
source: desk_book
dimensions:
  - name: index_name
    expr: index_name
  - name: kind
    expr: kind
measures:
  - name: premium_usd
    expr: cast(round(sum(premium * contracts * 100)) as bigint)
  - name: dollar_delta
    expr: cast(round(sum(delta * contracts * 100 * spot)) as bigint)
  - name: vega_usd_per_pt
    expr: cast(round(sum(vega_pt * contracts * 100)) as bigint)
  - name: theta_usd_per_day
    expr: cast(round(sum(theta_day * contracts * 100)) as bigint)
$$|}
      >>= fun _ ->
      let by_index =
        Dataframe.group_by (Session.table spark "desk_risk") [ col "index_name" ] |> fun g ->
        Dataframe.Grouped.agg g
          (List.map
             (fun m -> Column.alias (Column.fn "measure" [ col m ]) m)
             [ "premium_usd"; "dollar_delta"; "vega_usd_per_pt"; "theta_usd_per_day" ])
        |> fun d -> Dataframe.sort d [ col "index_name" ]
      in
      Dataframe.show by_index >>= fun () ->
      Dataframe.collect by_index >>= fun via_view ->
      Session.sql spark
        "SELECT index_name, cast(round(sum(premium * contracts * 100)) as bigint), cast(round(sum(delta * contracts * \
         100 * spot)) as bigint), cast(round(sum(vega_pt * contracts * 100)) as bigint), cast(round(sum(theta_day * \
         contracts * 100)) as bigint) FROM desk_book GROUP BY index_name ORDER BY index_name"
      >>= Dataframe.collect
      >>= fun direct ->
      check "metric view = direct aggregation on the book"
        (List.map Row.to_string via_view = List.map Row.to_string direct)
        "";
      (* --------------------------------------------------------------- 5. next day *)
      header "5. Next day: merge fills, audit yesterday's book by time travel";
      let history () =
        Session.sql spark "DESCRIBE HISTORY desk_book" >|= fun h ->
        Dataframe.sort (Dataframe.select_names h [ "version"; "operation" ]) [ col "version" ]
      in
      history () >>= Dataframe.collect >>= fun before ->
      let yesterday_version =
        List.fold_left (fun acc r -> max acc (Value.to_int64 (Row.field r "version"))) 0L before
      in
      let fills =
        Session.create_dataframe_ddl spark ~ddl:"index_name string, kind string, contracts int"
          ~columns:[ "index_name"; "kind"; "contracts" ]
          [
            [ Value.String "SP500"; Value.String "call"; Value.Int 25 ];
            [ Value.String "NASDAQCOM"; Value.String "call"; Value.Int 0 ];
          ]
      in
      Dataframe.create_or_replace_temp_view fills "desk_fills" >>= fun () ->
      Session.sql spark
        "MERGE INTO desk_book b USING desk_fills f ON b.index_name = f.index_name AND b.kind = f.kind WHEN MATCHED AND \
         f.contracts = 0 THEN DELETE WHEN MATCHED THEN UPDATE SET contracts = f.contracts"
      >>= fun _ ->
      history () >>= fun h ->
      Dataframe.show (Dataframe.filter h Column.O.(col "version" >= Column.lit (Value.Int64 yesterday_version)))
      >>= fun () ->
      let contracts q = Session.sql spark q >>= Dataframe.collect >|= List.map Row.to_string in
      contracts
        (Printf.sprintf "SELECT index_name, kind, contracts FROM desk_book VERSION AS OF %Ld ORDER BY index_name, kind"
           yesterday_version)
      >>= fun yesterday ->
      contracts "SELECT index_name, kind, contracts FROM desk_book ORDER BY index_name, kind" >>= fun today ->
      Printf.printf "  yesterday: %s\n  today:     %s\n" (String.concat " " yesterday) (String.concat " " today);
      check "time travel returns yesterday's book"
        (List.length yesterday = 4 && List.length today = 3)
        (Printf.sprintf "%d positions yesterday, %d today" (List.length yesterday) (List.length today));
      Printf.printf "\n%s\n%!"
        (if !failures = 0 then "all checks passed" else Printf.sprintf "%d check(s) failed" !failures);
      Session.close spark )
