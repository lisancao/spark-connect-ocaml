(* User-facing session: connection, SQL, readers, catalog, configuration. *)

open Lwt.Infix
module P = Spark_connect_proto
module R = P.Relations
module C = P.Commands
module B = P.Base
module Cat = P.Catalog

type t = { client : Client.t; conn : Grpc_client.t }

let client t = t.client
let session_id t = t.client.session_id
let server_side_session_id t = t.client.server_side_session_id

(* ---- connection strings: sc://host[:port]/;param=value;... ---- *)

type connection = {
  host : string;
  port : int;
  use_ssl : bool;
  token : string option;
  user_id : string option;
  session_id : string option;
  user_agent : string option;
  headers : (string * string) list;
  params : (string * string) list;
}

let parse_connection_string (s : string) : connection =
  let strip_prefix p s =
    if String.length s >= String.length p && String.sub s 0 (String.length p) = p then
      String.sub s (String.length p) (String.length s - String.length p)
    else invalid_arg ("connection string must start with " ^ p)
  in
  let rest = strip_prefix "sc://" s in
  let hostport, params_str =
    match String.index_opt rest '/' with
    | Some i -> (String.sub rest 0 i, String.sub rest (i + 1) (String.length rest - i - 1))
    | None -> (rest, "")
  in
  let host, port =
    match String.rindex_opt hostport ':' with
    | Some i when (not (String.contains hostport ']')) || i > String.rindex hostport ']' ->
        (String.sub hostport 0 i, int_of_string (String.sub hostport (i + 1) (String.length hostport - i - 1)))
    | _ -> (hostport, 15002)
  in
  let params =
    String.split_on_char ';' params_str
    |> List.filter_map (fun kv ->
        match String.index_opt kv '=' with
        | Some i -> Some (String.sub kv 0 i, String.sub kv (i + 1) (String.length kv - i - 1))
        | None -> if String.trim kv = "" then None else invalid_arg ("bad connection parameter: " ^ kv))
  in
  let get k = List.assoc_opt k params in
  let token = get "token" in
  let use_ssl = match get "use_ssl" with Some v -> String.lowercase_ascii v = "true" | None -> token <> None in
  let headers =
    List.filter_map (fun (k, v) -> if String.length k > 2 && String.sub k 0 2 = "x-" then Some (k, v) else None) params
  in
  {
    host;
    port;
    use_ssl;
    token;
    user_id = get "user_id";
    session_id = get "session_id";
    user_agent = get "user_agent";
    headers;
    params;
  }

(** Connect to a Spark Connect server. The argument is a Spark Connect connection string, for example
    ["sc://localhost:15002"] or ["sc://host.cloud:443/;token=abc;user_id=me"]. Explicit arguments override the string's
    parameters. *)
let connect ?user_id ?token ?use_ssl ?session_id ?(headers = []) (conn_string : string) : t Lwt.t =
  let c = parse_connection_string conn_string in
  let token = match token with Some t -> Some t | None -> c.token in
  let use_ssl = match use_ssl with Some b -> b | None -> c.use_ssl || (token <> None && c.params = []) in
  let user_id = match user_id with Some u -> Some u | None -> c.user_id in
  let session_id = match session_id with Some s -> Some s | None -> c.session_id in
  let auth = match token with Some t -> [ ("authorization", "Bearer " ^ t) ] | None -> [] in
  let grpc = Grpc_client.create ~headers:(auth @ c.headers @ headers) ~use_tls:use_ssl ~host:c.host ~port:c.port () in
  let client_type = match c.user_agent with Some ua -> ua | None -> "spark-connect-ocaml/0.1.0" in
  let client = Client.create ?session_id ?user_id ~client_type grpc in
  Lwt.return { client; conn = grpc }

(** Connect and verify the server is reachable (fetches its version). *)
let version t =
  Client.analyze t.client B.Spark_version >|= fun r ->
  match r.result with Some (B.Spark_version v) -> v.version | _ -> failwith "version: unexpected response"

let connect_and_check ?user_id ?token ?use_ssl ?session_id ?headers s =
  connect ?user_id ?token ?use_ssl ?session_id ?headers s >>= fun t ->
  version t >|= fun _ -> t

let close t = Client.close t.client

(* ---- SQL ---- *)

let of_rel_type t rt = Dataframe.of_rel_type t.client rt
let sql_args_pb (args : (string * Value.t) list) = List.map (fun (k, v) -> (k, Column.lit v)) args
let sql_pos_args_pb (args : Value.t list) = List.map Column.lit args

(** Run SQL. Commands (DDL, DML) execute eagerly; queries return a lazy DataFrame. [args] binds named parameters
    ([:name]), [pos_args] positional ones ([?]). *)
let sql ?(args : (string * Value.t) list = []) ?(pos_args : Value.t list = []) t (query : string) : Dataframe.t Lwt.t =
  let cmd =
    C.Sql_command
      (C.make_sql_command ~sql:query ~named_arguments:(sql_args_pb args) ~pos_arguments:(sql_pos_args_pb pos_args) ())
  in
  Client.execute t.client (B.Command cmd) >>= fun stream ->
  let result = ref None in
  Lwt_stream.iter
    (fun (r : B.execute_plan_response) ->
      match r.response_type with
      | Some (B.Sql_command_result res) -> ( match res.relation with Some rel -> result := Some rel | None -> ())
      | _ -> ())
    stream
  >|= fun () ->
  match !result with
  | Some rel -> Dataframe.of_relation t.client rel
  | None ->
      of_rel_type t
        (R.Sql (R.make_sql ~query ~named_arguments:(sql_args_pb args) ~pos_arguments:(sql_pos_args_pb pos_args) ()))

(* ---- sources ---- *)

let range ?(start = 0L) ?(step = 1L) ?num_partitions t end_ =
  of_rel_type t (R.Range (R.make_range ~start ~end_ ~step ?num_partitions:(Option.map Int32.of_int num_partitions) ()))

let table ?(options = []) t name =
  of_rel_type t
    (R.Read (R.make_read ~read_type:(R.Named_table (R.make_read_named_table ~unparsed_identifier:name ~options ())) ()))

module Reader = struct
  type session = t
  type t = { session : session; format : string option; schema : string option; options : (string * string) list }

  let format r f = { r with format = Some f }
  let schema r s = { r with schema = Some s }
  let option r k v = { r with options = r.options @ [ (k, v) ] }
  let options r kvs = { r with options = r.options @ kvs }

  let load_paths r paths =
    of_rel_type r.session
      (R.Read
         (R.make_read
            ~read_type:
              (R.Data_source (R.make_read_data_source ?format:r.format ?schema:r.schema ~options:r.options ~paths ()))
            ()))

  let load ?path r = load_paths r (Option.to_list path)
  let table r name = table ~options:r.options r.session name
  let parquet r paths = load_paths (format r "parquet") paths
  let csv r paths = load_paths (format r "csv") paths
  let json r paths = load_paths (format r "json") paths
  let orc r paths = load_paths (format r "orc") paths
  let text r paths = load_paths (format r "text") paths
  let jdbc r ~url ~table = load (options (format r "jdbc") [ ("url", url); ("dbtable", table) ])
end

let read t : Reader.t = { session = t; format = None; schema = None; options = [] }

(** Build a DataFrame from local rows. Types are inferred from the values unless [schema] (an Arrow schema) is given. *)
let create_dataframe ?schema t ~(columns : string list) (rows : Value.t list list) : Dataframe.t =
  let data = Arrow_writer.encode ?schema ~columns rows in
  of_rel_type t (R.Local_relation (R.make_local_relation ~data:(Bytes.unsafe_of_string data) ()))

(** Same, with an explicit Spark DDL schema such as ["id INT, name STRING"]. *)
let create_dataframe_ddl t ~(ddl : string) ~(columns : string list) rows : Dataframe.t =
  let data = Arrow_writer.encode ~columns rows in
  of_rel_type t (R.Local_relation (R.make_local_relation ~data:(Bytes.unsafe_of_string data) ~schema:ddl ()))

(* ---- configuration ---- *)

module Conf = struct
  let pairs (r : B.config_response) =
    List.map (fun (kv : B.key_value) -> (kv.key, if B.key_value_has_value kv then Some kv.value else None)) r.pairs

  let set t key value =
    Client.config t.client (B.Set (B.make_config_request_set ~pairs:[ B.make_key_value ~key ~value () ] ()))
    >|= fun _ -> ()

  let get t key =
    Client.config t.client (B.Get (B.make_config_request_get ~keys:[ key ] ())) >|= fun r ->
    match pairs r with (_, Some v) :: _ -> v | _ -> raise Not_found

  let get_opt t key =
    Client.config t.client (B.Get_option (B.make_config_request_get_option ~keys:[ key ] ())) >|= fun r ->
    match pairs r with (_, v) :: _ -> v | _ -> None

  let get_with_default t key default =
    Client.config t.client
      (B.Get_with_default (B.make_config_request_get_with_default ~pairs:[ B.make_key_value ~key ~value:default () ] ()))
    >|= fun r -> match pairs r with (_, Some v) :: _ -> v | _ -> default

  (** All settings. With [prefix], Spark returns the matching keys with the prefix removed (e.g. [".partitions"] for
      prefix ["spark.sql.shuffle"]). *)
  let get_all ?prefix t =
    Client.config t.client (B.Get_all (B.make_config_request_get_all ?prefix ())) >|= fun r ->
    List.map (fun (k, v) -> (k, Option.value ~default:"" v)) (pairs r)

  let unset t key = Client.config t.client (B.Unset (B.make_config_request_unset ~keys:[ key ] ())) >|= fun _ -> ()

  let is_modifiable t key =
    Client.config t.client (B.Is_modifiable (B.make_config_request_is_modifiable ~keys:[ key ] ())) >|= fun r ->
    match pairs r with (_, Some "true") :: _ -> true | _ -> false
end

(* ---- catalog ---- *)

module Catalog = struct
  let df t (cat : Cat.catalog) = of_rel_type t (R.Catalog cat)
  let scalar_string t cat = Dataframe.collect (df t cat) >|= function r :: _ -> Value.to_str (Row.get r 0) | [] -> ""

  let scalar_bool t cat =
    Dataframe.collect (df t cat) >|= function r :: _ -> Value.to_bool (Row.get r 0) | [] -> false

  let unit t cat = Dataframe.collect (df t cat) >|= fun _ -> ()
  let current_catalog t = scalar_string t Cat.Current_catalog
  let set_current_catalog t name = unit t (Cat.Set_current_catalog (Cat.make_set_current_catalog ~catalog_name:name ()))
  let list_catalogs ?pattern t = df t (Cat.List_catalogs (Cat.make_list_catalogs ?pattern ()))
  let current_database t = scalar_string t Cat.Current_database
  let set_current_database t name = unit t (Cat.Set_current_database (Cat.make_set_current_database ~db_name:name ()))
  let list_databases ?pattern t = df t (Cat.List_databases (Cat.make_list_databases ?pattern ()))
  let list_tables ?db_name ?pattern t = df t (Cat.List_tables (Cat.make_list_tables ?db_name ?pattern ()))
  let list_functions ?db_name ?pattern t = df t (Cat.List_functions (Cat.make_list_functions ?db_name ?pattern ()))
  let list_columns ?db_name t table_name = df t (Cat.List_columns (Cat.make_list_columns ~table_name ?db_name ()))
  let database_exists t db_name = scalar_bool t (Cat.Database_exists (Cat.make_database_exists ~db_name ()))

  let table_exists ?db_name t table_name =
    scalar_bool t (Cat.Table_exists (Cat.make_table_exists ~table_name ?db_name ()))

  let function_exists ?db_name t function_name =
    scalar_bool t (Cat.Function_exists (Cat.make_function_exists ~function_name ?db_name ()))

  let drop_temp_view t view_name = scalar_bool t (Cat.Drop_temp_view (Cat.make_drop_temp_view ~view_name ()))

  let drop_global_temp_view t view_name =
    scalar_bool t (Cat.Drop_global_temp_view (Cat.make_drop_global_temp_view ~view_name ()))

  let is_cached t table_name = scalar_bool t (Cat.Is_cached (Cat.make_is_cached ~table_name ()))
  let cache_table t table_name = unit t (Cat.Cache_table (Cat.make_cache_table ~table_name ()))
  let uncache_table t table_name = unit t (Cat.Uncache_table (Cat.make_uncache_table ~table_name ()))
  let clear_cache t = unit t Cat.Clear_cache
  let refresh_table t table_name = unit t (Cat.Refresh_table (Cat.make_refresh_table ~table_name ()))
  let refresh_by_path t path = unit t (Cat.Refresh_by_path (Cat.make_refresh_by_path ~path ()))
  let recover_partitions t table_name = unit t (Cat.Recover_partitions (Cat.make_recover_partitions ~table_name ()))
end

(* ---- control ---- *)

let interrupt_all t = Client.interrupt t.client B.Interrupt_type_all ()
let interrupt_tag t tag = Client.interrupt t.client B.Interrupt_type_tag ~interrupt:(B.Operation_tag tag) ()
let interrupt_operation t id = Client.interrupt t.client B.Interrupt_type_operation_id ~interrupt:(B.Operation_id id) ()

(** Full error details for a [Spark_error] that carries an error id: error classes, SQL state, message parameters and
    server stack trace. *)
let fetch_error_details t (e : Error.t) =
  match e.error_id with
  | None -> Lwt.return_none
  | Some error_id -> Client.fetch_error_details t.client ~error_id >|= fun r -> Some r
