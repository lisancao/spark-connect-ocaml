(* Low-level session client: owns the gRPC channel and session identity, and
   exposes the Spark Connect RPCs on raw protobuf messages. *)

open Lwt.Infix
module P = Spark_connect_proto

let service = "/spark.connect.SparkConnectService/"

type t = {
  grpc : Grpc_client.t;
  session_id : string;
  user_id : string;
  client_type : string;
  mutable server_side_session_id : string option;
  mutable next_plan_id : int64;
}

let uuid4 () =
  let b = Bytes.init 16 (fun _ -> Char.chr (Random.int 256)) in
  Bytes.set b 6 (Char.chr (0x40 lor (Char.code (Bytes.get b 6) land 0x0f)));
  Bytes.set b 8 (Char.chr (0x80 lor (Char.code (Bytes.get b 8) land 0x3f)));
  let h i = Printf.sprintf "%02x" (Char.code (Bytes.get b i)) in
  String.concat ""
    [ h 0; h 1; h 2; h 3; "-"; h 4; h 5; "-"; h 6; h 7; "-"; h 8; h 9; "-"; h 10; h 11; h 12; h 13; h 14; h 15 ]

let () = Random.self_init ()

let create ?session_id ?(user_id = "ocaml") ?(client_type = "spark-connect-ocaml/0.1.0") grpc =
  let session_id = match session_id with Some s -> s | None -> uuid4 () in
  { grpc; session_id; user_id; client_type; server_side_session_id = None; next_plan_id = 0L }

let fresh_plan_id t =
  let id = t.next_plan_id in
  t.next_plan_id <- Int64.succ id;
  id

let user_context t = P.Base.make_user_context ~user_id:t.user_id ()

let encode f v =
  let e = Pbrt.Encoder.create () in
  f v e;
  Pbrt.Encoder.to_string e

let decode f s = f (Pbrt.Decoder.of_string s)
let observe t ssid = if ssid <> "" then t.server_side_session_id <- Some ssid

let unary t ~method_ ~encode:enc ~decode:dec req =
  Error.wrap (fun () -> Grpc_client.unary t.grpc ~path:(service ^ method_) ~payload:(encode enc req) >|= decode dec)

(** Execute a plan; the stream yields every [ExecutePlanResponse]. *)
let execute ?operation_id t (plan : P.Base.plan) : P.Base.execute_plan_response Lwt_stream.t Lwt.t =
  let req =
    P.Base.make_execute_plan_request ~session_id:t.session_id
      ?client_observed_server_side_session_id:t.server_side_session_id ~user_context:(user_context t)
      ~operation_id:(match operation_id with Some o -> o | None -> uuid4 ())
      ~plan ~client_type:t.client_type ()
  in
  Error.wrap (fun () ->
      Grpc_client.call t.grpc ~path:(service ^ "ExecutePlan")
        ~payload:(encode P.Base.encode_pb_execute_plan_request req)
      >|= fun raw ->
      Lwt_stream.from (fun () ->
          Error.wrap (fun () ->
              Lwt_stream.get raw >|= function
              | None -> None
              | Some m ->
                  let r = decode P.Base.decode_pb_execute_plan_response m in
                  observe t r.server_side_session_id;
                  Some r)))

(** Execute and discard everything but errors. *)
let execute_unit t plan = execute t plan >>= fun s -> Lwt_stream.junk_while (fun _ -> true) s

let analyze t (analyze : P.Base.analyze_plan_request_analyze) : P.Base.analyze_plan_response Lwt.t =
  let req =
    P.Base.make_analyze_plan_request ~session_id:t.session_id
      ?client_observed_server_side_session_id:t.server_side_session_id ~user_context:(user_context t)
      ~client_type:t.client_type ~analyze ()
  in
  unary t ~method_:"AnalyzePlan" ~encode:P.Base.encode_pb_analyze_plan_request
    ~decode:P.Base.decode_pb_analyze_plan_response req
  >|= fun r ->
  observe t r.server_side_session_id;
  r

let config t (operation : P.Base.config_request_operation) : P.Base.config_response Lwt.t =
  let req =
    P.Base.make_config_request ~session_id:t.session_id ?client_observed_server_side_session_id:t.server_side_session_id
      ~user_context:(user_context t) ~client_type:t.client_type ~operation ()
  in
  unary t ~method_:"Config" ~encode:P.Base.encode_pb_config_request ~decode:P.Base.decode_pb_config_response req
  >|= fun r ->
  observe t r.server_side_session_id;
  r

let interrupt t (interrupt_type : P.Base.interrupt_request_interrupt_type)
    ?(interrupt : P.Base.interrupt_request_interrupt option) () =
  let req =
    P.Base.make_interrupt_request ~session_id:t.session_id
      ?client_observed_server_side_session_id:t.server_side_session_id ~user_context:(user_context t)
      ~client_type:t.client_type ~interrupt_type ?interrupt ()
  in
  unary t ~method_:"Interrupt" ~encode:P.Base.encode_pb_interrupt_request ~decode:P.Base.decode_pb_interrupt_response
    req
  >|= fun r -> r.interrupted_ids

let fetch_error_details t ~error_id =
  let req =
    P.Base.make_fetch_error_details_request ~session_id:t.session_id
      ?client_observed_server_side_session_id:t.server_side_session_id ~user_context:(user_context t)
      ~client_type:t.client_type ~error_id ()
  in
  unary t ~method_:"FetchErrorDetails" ~encode:P.Base.encode_pb_fetch_error_details_request
    ~decode:P.Base.decode_pb_fetch_error_details_response req

let release_session ?(allow_reconnect = false) t =
  let req =
    P.Base.make_release_session_request ~session_id:t.session_id ~user_context:(user_context t)
      ~client_type:t.client_type ~allow_reconnect ()
  in
  unary t ~method_:"ReleaseSession" ~encode:P.Base.encode_pb_release_session_request
    ~decode:P.Base.decode_pb_release_session_response req
  >|= fun _ -> ()

let close t = release_session t >>= fun () -> Grpc_client.close t.grpc
