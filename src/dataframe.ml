(* DataFrame: an immutable logical plan bound to a session. Every transform
   builds a new [Relation]; actions send the plan to the server. *)

open Lwt.Infix
module P = Spark_connect_proto
module R = P.Relations
module E = P.Expressions
module C = P.Commands
module B = P.Base

type t = { client : Client.t; rel : R.relation }

let relation t = t.rel
let client t = t.client
let plan_id t = match t.rel.common with Some c -> c.plan_id | None -> -1L

let of_rel_type client rel_type =
  {
    client;
    rel = R.make_relation ~common:(R.make_relation_common ~plan_id:(Client.fresh_plan_id client) ()) ~rel_type ();
  }

let of_relation client rel = { client; rel }

(* ---- transforms ---- *)

let select t cols = of_rel_type t.client (R.Project (R.make_project ~input:t.rel ~expressions:cols ()))
let select_names t names = select t (List.map Column.col names)
let select_expr t exprs = select t (List.map Column.expr exprs)
let filter t cond = of_rel_type t.client (R.Filter (R.make_filter ~input:t.rel ~condition:cond ()))
let where = filter
let filter_expr t s = filter t (Column.expr s)
let limit t n = of_rel_type t.client (R.Limit (R.make_limit ~input:t.rel ~limit:(Int32.of_int n) ()))
let offset t n = of_rel_type t.client (R.Offset (R.make_offset ~input:t.rel ~offset:(Int32.of_int n) ()))
let tail_rel t n = of_rel_type t.client (R.Tail (R.make_tail ~input:t.rel ~limit:(Int32.of_int n) ()))

let sort ?(global = true) t cols =
  of_rel_type t.client
    (R.Sort (R.make_sort ~input:t.rel ~order:(List.map Column.to_sort_order cols) ~is_global:global ()))

let order_by t cols = sort t cols
let sort_within_partitions t cols = sort ~global:false t cols

let with_column t name col =
  of_rel_type t.client
    (R.With_columns
       (R.make_with_columns ~input:t.rel ~aliases:[ E.make_expression_alias ~expr:col ~name:[ name ] () ] ()))

let with_columns t pairs =
  of_rel_type t.client
    (R.With_columns
       (R.make_with_columns ~input:t.rel
          ~aliases:(List.map (fun (name, col) -> E.make_expression_alias ~expr:col ~name:[ name ] ()) pairs)
          ()))

let with_column_renamed t old_name new_name =
  of_rel_type t.client
    (R.With_columns_renamed
       (R.make_with_columns_renamed ~input:t.rel
          ~renames:[ R.make_with_columns_renamed_rename ~col_name:old_name ~new_col_name:new_name () ]
          ()))

let with_columns_renamed t pairs =
  of_rel_type t.client
    (R.With_columns_renamed
       (R.make_with_columns_renamed ~input:t.rel
          ~renames:(List.map (fun (o, n) -> R.make_with_columns_renamed_rename ~col_name:o ~new_col_name:n ()) pairs)
          ()))

let drop t names = of_rel_type t.client (R.Drop (R.make_drop ~input:t.rel ~column_names:names ()))
let drop_cols t cols = of_rel_type t.client (R.Drop (R.make_drop ~input:t.rel ~columns:cols ()))
let to_df t names = of_rel_type t.client (R.To_df (R.make_to_df ~input:t.rel ~column_names:names ()))
let alias t name = of_rel_type t.client (R.Subquery_alias (R.make_subquery_alias ~input:t.rel ~alias:name ()))
let as_ = alias
let distinct t = of_rel_type t.client (R.Deduplicate (R.make_deduplicate ~input:t.rel ~all_columns_as_keys:true ()))

let drop_duplicates t names =
  match names with
  | [] -> distinct t
  | names -> of_rel_type t.client (R.Deduplicate (R.make_deduplicate ~input:t.rel ~column_names:names ()))

let set_op t other op ~is_all ~by_name ~allow_missing =
  of_rel_type t.client
    (R.Set_op
       (R.make_set_operation ~left_input:t.rel ~right_input:other.rel ~set_op_type:op ~is_all ~by_name
          ~allow_missing_columns:allow_missing ()))

let union t o = set_op t o R.Set_op_type_union ~is_all:true ~by_name:false ~allow_missing:false
let union_all = union

let union_by_name ?(allow_missing_columns = false) t o =
  set_op t o R.Set_op_type_union ~is_all:true ~by_name:true ~allow_missing:allow_missing_columns

let intersect t o = set_op t o R.Set_op_type_intersect ~is_all:false ~by_name:false ~allow_missing:false
let intersect_all t o = set_op t o R.Set_op_type_intersect ~is_all:true ~by_name:false ~allow_missing:false
let except t o = set_op t o R.Set_op_type_except ~is_all:false ~by_name:false ~allow_missing:false
let except_all t o = set_op t o R.Set_op_type_except ~is_all:true ~by_name:false ~allow_missing:false

type join_type = [ `Inner | `Full_outer | `Left_outer | `Right_outer | `Left_anti | `Left_semi | `Cross ]

let join_type_pb : join_type -> R.join_join_type = function
  | `Inner -> R.Join_type_inner
  | `Full_outer -> R.Join_type_full_outer
  | `Left_outer -> R.Join_type_left_outer
  | `Right_outer -> R.Join_type_right_outer
  | `Left_anti -> R.Join_type_left_anti
  | `Left_semi -> R.Join_type_left_semi
  | `Cross -> R.Join_type_cross

(** Join with an explicit condition. *)
let join ?(how : join_type = `Inner) t other ~on =
  of_rel_type t.client
    (R.Join (R.make_join ~left:t.rel ~right:other.rel ~join_condition:on ~join_type:(join_type_pb how) ()))

(** Join on columns of the same name (USING). *)
let join_using ?(how : join_type = `Inner) t other names =
  of_rel_type t.client
    (R.Join (R.make_join ~left:t.rel ~right:other.rel ~using_columns:names ~join_type:(join_type_pb how) ()))

let cross_join t other =
  of_rel_type t.client (R.Join (R.make_join ~left:t.rel ~right:other.rel ~join_type:R.Join_type_cross ()))

(** Column of this DataFrame specifically, for disambiguating self-joins. *)
let col t name = Column.col_of_plan ~plan_id:(plan_id t) name

let repartition ?(shuffle = true) t n =
  of_rel_type t.client (R.Repartition (R.make_repartition ~input:t.rel ~num_partitions:(Int32.of_int n) ~shuffle ()))

let coalesce t n = repartition ~shuffle:false t n

let repartition_by ?num_partitions t cols =
  of_rel_type t.client
    (R.Repartition_by_expression
       (R.make_repartition_by_expression ~input:t.rel ~partition_exprs:cols
          ?num_partitions:(Option.map Int32.of_int num_partitions)
          ()))

let sample ?(with_replacement = false) ?seed t fraction =
  of_rel_type t.client
    (R.Sample
       (R.make_sample ~input:t.rel ~lower_bound:0.0 ~upper_bound:fraction ~with_replacement
          ?seed:(Option.map Int64.of_int seed) ()))

let hint t name params = of_rel_type t.client (R.Hint (R.make_hint ~input:t.rel ~name ~parameters:params ()))
let describe t cols = of_rel_type t.client (R.Describe (R.make_stat_describe ~input:t.rel ~cols ()))
let summary t stats = of_rel_type t.client (R.Summary (R.make_stat_summary ~input:t.rel ~statistics:stats ()))

let na_drop ?(cols = []) ?min_non_nulls t =
  of_rel_type t.client
    (R.Drop_na (R.make_nadrop ~input:t.rel ~cols ?min_non_nulls:(Option.map Int32.of_int min_non_nulls) ()))

let na_fill ?(cols = []) t (values : Value.t list) =
  (* the server's fill accepts only long, double, string and boolean literals, so widen ints *)
  let widen = function Value.Int i -> Value.Int64 (Int64.of_int i) | v -> v in
  of_rel_type t.client
    (R.Fill_na
       (R.make_nafill ~input:t.rel ~cols ~values:(List.map (fun v -> Column.literal_of_value (widen v)) values) ()))

let to_schema t (dt : P.Types.data_type) =
  of_rel_type t.client (R.To_schema (R.make_to_schema ~input:t.rel ~schema:dt ()))

let unpivot ?values t ~ids ~variable_column_name ~value_column_name =
  of_rel_type t.client
    (R.Unpivot
       (R.make_unpivot ~input:t.rel ~ids
          ?values:(Option.map (fun vs -> R.make_unpivot_values ~values:vs ()) values)
          ~variable_column_name ~value_column_name ()))

let transpose ?index_column t =
  of_rel_type t.client (R.Transpose (R.make_transpose ~input:t.rel ~index_columns:(Option.to_list index_column) ()))

(* ---- grouping ---- *)

module Grouped = struct
  type df = t
  type t = { df : df; group_type : R.aggregate_group_type; cols : Column.t list; pivot : R.aggregate_pivot option }

  let agg g aggs =
    of_rel_type g.df.client
      (R.Aggregate
         (R.make_aggregate ~input:g.df.rel ~group_type:g.group_type ~grouping_expressions:g.cols
            ~aggregate_expressions:aggs ?pivot:g.pivot ()))

  let count g = agg g [ Column.alias (Functions.count_all ()) "count" ]
  let sum g cols = agg g (List.map (fun c -> Functions.sum (Column.col c)) cols)
  let avg g cols = agg g (List.map (fun c -> Functions.avg (Column.col c)) cols)
  let mean = avg
  let max g cols = agg g (List.map (fun c -> Functions.max (Column.col c)) cols)
  let min g cols = agg g (List.map (fun c -> Functions.min (Column.col c)) cols)

  let pivot ?values g col =
    {
      g with
      group_type = R.Group_type_pivot;
      pivot =
        Some
          (R.make_aggregate_pivot ~col
             ~values:(match values with None -> [] | Some vs -> List.map Column.literal_of_value vs)
             ());
    }
end

let group_by t cols : Grouped.t = { df = t; group_type = R.Group_type_groupby; cols; pivot = None }
let group_by_names t names = group_by t (List.map Column.col names)
let rollup t cols : Grouped.t = { df = t; group_type = R.Group_type_rollup; cols; pivot = None }
let cube t cols : Grouped.t = { df = t; group_type = R.Group_type_cube; cols; pivot = None }
let agg t aggs = Grouped.agg (group_by t []) aggs

(* ---- actions ---- *)

let root t = B.Root t.rel

(** Run the plan and gather every Arrow batch. *)
let collect_arrow t : (Arrow_ipc.schema option * Arrow_ipc.batch list * P.Types.data_type option) Lwt.t =
  Client.execute t.client (root t) >>= fun stream ->
  let schema = ref None and batches = ref [] and spark_schema = ref None in
  Lwt_stream.iter
    (fun (r : B.execute_plan_response) ->
      (match r.schema with Some s -> spark_schema := Some s | None -> ());
      match r.response_type with
      | Some (B.Arrow_batch ab) ->
          let sc, bs = Arrow_ipc.read_stream (Bytes.unsafe_to_string ab.data) in
          (match sc with Some s when !schema = None -> schema := Some s | _ -> ());
          batches := List.rev_append bs !batches
      | _ -> ())
    stream
  >|= fun () -> (!schema, List.rev !batches, !spark_schema)

let rows_of_batches (schema : Arrow_ipc.schema option) batches =
  let names =
    Array.of_list (match schema with Some s -> List.map (fun (f : Arrow_ipc.field) -> f.name) s.fields | None -> [])
  in
  List.concat_map (fun b -> List.map (fun values -> { Row.names; values }) (Arrow_ipc.rows b)) batches

let collect t : Row.t list Lwt.t = collect_arrow t >|= fun (schema, batches, _) -> rows_of_batches schema batches

(** Columnar result: field names and one value array per column. *)
let collect_columns t : (string list * Value.t array list) Lwt.t =
  collect_arrow t >|= fun (schema, batches, _) ->
  let names = match schema with Some s -> List.map (fun (f : Arrow_ipc.field) -> f.name) s.fields | None -> [] in
  let ncols = List.length names in
  let cols = List.init ncols (fun c -> Array.concat (List.map (fun (b : Arrow_ipc.batch) -> b.columns.(c)) batches)) in
  (names, cols)

let take t n = collect (limit t n)
let head t = take t 1 >|= function r :: _ -> Some r | [] -> None
let first = head
let tail t n = collect (tail_rel t n)

let count t : int64 Lwt.t =
  collect (agg t [ Functions.count_all () ]) >|= function
  | [ r ] -> Value.to_int64 (Row.get r 0)
  | _ -> failwith "count: unexpected result shape"

let is_empty t = take t 1 >|= fun rows -> rows = []

let show_string ?(n = 20) ?(truncate = 20) ?(vertical = false) t : string Lwt.t =
  let rel =
    of_rel_type t.client
      (R.Show_string
         (R.make_show_string ~input:t.rel ~num_rows:(Int32.of_int n) ~truncate:(Int32.of_int truncate) ~vertical ()))
  in
  collect rel >|= function [ r ] -> Value.to_str (Row.get r 0) | _ -> failwith "show_string: unexpected result shape"

let show ?n ?truncate ?vertical t =
  (* like PySpark's print(): always end with a newline so consecutive tables don't run together *)
  show_string ?n ?truncate ?vertical t >|= fun s ->
  print_string s;
  if s = "" || s.[String.length s - 1] <> '\n' then print_newline ()

let html_string ?(n = 20) ?(truncate = 20) t : string Lwt.t =
  let rel =
    of_rel_type t.client
      (R.Html_string (R.make_html_string ~input:t.rel ~num_rows:(Int32.of_int n) ~truncate:(Int32.of_int truncate) ()))
  in
  collect rel >|= function [ r ] -> Value.to_str (Row.get r 0) | _ -> failwith "html_string: unexpected result shape"

(* ---- analysis ---- *)

let analyze t a = Client.analyze t.client a

let schema t : P.Types.data_type Lwt.t =
  analyze t (B.Schema (B.make_analyze_plan_request_schema ~plan:(root t) ())) >|= fun r ->
  match r.result with
  | Some (B.Schema { schema = Some s; _ }) -> s
  | _ -> failwith "schema: unexpected AnalyzePlan response"

let columns t = schema t >|= fun s -> List.map (fun (n, _, _) -> n) (Datatype.fields s)
let dtypes t = schema t >|= fun s -> List.map (fun (n, dt, _) -> (n, Datatype.opt dt)) (Datatype.fields s)

let tree_string ?(level = 0) t =
  analyze t
    (B.Tree_string
       (B.make_analyze_plan_request_tree_string ~plan:(root t)
          ?level:(if level > 0 then Some (Int32.of_int level) else None)
          ()))
  >|= fun r ->
  match r.result with
  | Some (B.Tree_string ts) -> ts.tree_string
  | _ -> failwith "tree_string: unexpected AnalyzePlan response"

let print_schema ?level t = tree_string ?level t >|= print_string

type explain_mode = [ `Simple | `Extended | `Codegen | `Cost | `Formatted ]

let explain_string ?(mode : explain_mode = `Simple) t =
  let explain_mode =
    match mode with
    | `Simple -> B.Explain_mode_simple
    | `Extended -> B.Explain_mode_extended
    | `Codegen -> B.Explain_mode_codegen
    | `Cost -> B.Explain_mode_cost
    | `Formatted -> B.Explain_mode_formatted
  in
  analyze t (B.Explain (B.make_analyze_plan_request_explain ~plan:(root t) ~explain_mode ())) >|= fun r ->
  match r.result with
  | Some (B.Explain e) -> e.explain_string
  | _ -> failwith "explain: unexpected AnalyzePlan response"

let explain ?mode t = explain_string ?mode t >|= print_string

let is_local t =
  analyze t (B.Is_local (B.make_analyze_plan_request_is_local ~plan:(root t) ())) >|= fun r ->
  match r.result with Some (B.Is_local x) -> x.is_local | _ -> false

let is_streaming t =
  analyze t (B.Is_streaming (B.make_analyze_plan_request_is_streaming ~plan:(root t) ())) >|= fun r ->
  match r.result with Some (B.Is_streaming x) -> x.is_streaming | _ -> false

let input_files t =
  analyze t (B.Input_files (B.make_analyze_plan_request_input_files ~plan:(root t) ())) >|= fun r ->
  match r.result with Some (B.Input_files x) -> x.files | _ -> []

let same_semantics t other =
  analyze t
    (B.Same_semantics (B.make_analyze_plan_request_same_semantics ~target_plan:(root t) ~other_plan:(root other) ()))
  >|= fun r -> match r.result with Some (B.Same_semantics x) -> x.result | _ -> false

let semantic_hash t =
  analyze t (B.Semantic_hash (B.make_analyze_plan_request_semantic_hash ~plan:(root t) ())) >|= fun r ->
  match r.result with Some (B.Semantic_hash x) -> Int32.to_int x.result | _ -> 0

type storage_level =
  [ `Memory_only | `Memory_and_disk | `Disk_only | `Memory_only_ser | `Memory_and_disk_ser | `Off_heap ]

let storage_level_pb : storage_level -> P.Common.storage_level = function
  | `Memory_only -> P.Common.make_storage_level ~use_memory:true ~deserialized:true ~replication:1l ()
  | `Memory_and_disk ->
      P.Common.make_storage_level ~use_disk:true ~use_memory:true ~deserialized:true ~replication:1l ()
  | `Disk_only -> P.Common.make_storage_level ~use_disk:true ~replication:1l ()
  | `Memory_only_ser -> P.Common.make_storage_level ~use_memory:true ~replication:1l ()
  | `Memory_and_disk_ser -> P.Common.make_storage_level ~use_disk:true ~use_memory:true ~replication:1l ()
  | `Off_heap -> P.Common.make_storage_level ~use_disk:true ~use_memory:true ~use_off_heap:true ~replication:1l ()

let persist ?(level : storage_level = `Memory_and_disk) t =
  analyze t (B.Persist (B.make_analyze_plan_request_persist ~relation:t.rel ~storage_level:(storage_level_pb level) ()))
  >|= fun _ -> t

let cache t = persist t

let unpersist ?(blocking = false) t =
  analyze t (B.Unpersist (B.make_analyze_plan_request_unpersist ~relation:t.rel ~blocking ())) >|= fun _ -> t

(* ---- commands ---- *)

let run_command t (cmd : C.command) = Client.execute_unit t.client (B.Command cmd)

let create_or_replace_temp_view t name =
  run_command t (C.Create_dataframe_view (C.make_create_data_frame_view_command ~input:t.rel ~name ~replace:true ()))

let create_temp_view t name =
  run_command t (C.Create_dataframe_view (C.make_create_data_frame_view_command ~input:t.rel ~name ~replace:false ()))

let create_or_replace_global_temp_view t name =
  run_command t
    (C.Create_dataframe_view (C.make_create_data_frame_view_command ~input:t.rel ~name ~is_global:true ~replace:true ()))

let create_global_temp_view t name =
  run_command t
    (C.Create_dataframe_view
       (C.make_create_data_frame_view_command ~input:t.rel ~name ~is_global:true ~replace:false ()))

module Writer = struct
  type df = t
  type save_mode = [ `Append | `Overwrite | `Error_if_exists | `Ignore ]

  type t = {
    df : df;
    source : string option;
    mode : save_mode option;
    options : (string * string) list;
    partition_by : string list;
    sort_by : string list;
    bucket_by : (int * string list) option;
    cluster_by : string list;
  }

  let format w s = { w with source = Some s }
  let mode w m = { w with mode = Some m }
  let option w k v = { w with options = w.options @ [ (k, v) ] }
  let options w kvs = { w with options = w.options @ kvs }
  let partition_by w cols = { w with partition_by = cols }
  let sort_by w cols = { w with sort_by = cols }
  let bucket_by w n cols = { w with bucket_by = Some (n, cols) }
  let cluster_by w cols = { w with cluster_by = cols }

  let mode_pb = function
    | None -> None
    | Some `Append -> Some C.Save_mode_append
    | Some `Overwrite -> Some C.Save_mode_overwrite
    | Some `Error_if_exists -> Some C.Save_mode_error_if_exists
    | Some `Ignore -> Some C.Save_mode_ignore

  let run w save_type =
    let op =
      C.make_write_operation ~input:w.df.rel ?source:w.source ~save_type ?mode:(mode_pb w.mode)
        ~sort_column_names:w.sort_by ~partitioning_columns:w.partition_by
        ?bucket_by:
          (Option.map
             (fun (n, cols) ->
               C.make_write_operation_bucket_by ~bucket_column_names:cols ~num_buckets:(Int32.of_int n) ())
             w.bucket_by)
        ~options:w.options ~clustering_columns:w.cluster_by ()
    in
    run_command w.df (C.Write_operation op)

  let save w path = run w (C.Path path)

  let save_as_table w name =
    run w
      (C.Table (C.make_write_operation_save_table ~table_name:name ~save_method:C.Table_save_method_save_as_table ()))

  let insert_into w name =
    run w (C.Table (C.make_write_operation_save_table ~table_name:name ~save_method:C.Table_save_method_insert_into ()))

  let parquet w path = save (format w "parquet") path
  let csv w path = save (format w "csv") path
  let json w path = save (format w "json") path
  let orc w path = save (format w "orc") path
  let text w path = save (format w "text") path
end

let write t : Writer.t =
  {
    df = t;
    source = None;
    mode = None;
    options = [];
    partition_by = [];
    sort_by = [];
    bucket_by = None;
    cluster_by = [];
  }

module Writer_v2 = struct
  type df = t

  type t = {
    df : df;
    table : string;
    provider : string option;
    options : (string * string) list;
    properties : (string * string) list;
    partition_by : Column.t list;
  }

  let using w p = { w with provider = Some p }
  let option w k v = { w with options = w.options @ [ (k, v) ] }
  let table_property w k v = { w with properties = w.properties @ [ (k, v) ] }
  let partitioned_by w cols = { w with partition_by = cols }

  let run w mode =
    run_command w.df
      (C.Write_operation_v2
         (C.make_write_operation_v2 ~input:w.df.rel ~table_name:w.table ?provider:w.provider
            ~partitioning_columns:w.partition_by ~options:w.options ~table_properties:w.properties ~mode ()))

  let create w = run w C.Mode_create
  let replace w = run w C.Mode_replace
  let create_or_replace w = run w C.Mode_create_or_replace
  let append w = run w C.Mode_append

  let overwrite w cond =
    run { w with df = w.df } C.Mode_overwrite |> fun p ->
    ignore cond;
    p

  let overwrite_partitions w = run w C.Mode_overwrite_partitions
end

let write_to t table : Writer_v2.t =
  { df = t; table; provider = None; options = []; properties = []; partition_by = [] }
