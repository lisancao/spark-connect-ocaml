(* gRPC client over HTTP/2 (h2), with just enough of the protocol for
   Spark Connect: unary and server-streaming calls, length-prefixed protobuf
   messages, trailer-based status, and base64 error details. *)

open Lwt.Infix

type status = {
  code : int;
  message : string;
  details : string option;  (** raw [google.rpc.Status] bytes from [grpc-status-details-bin], if any *)
}

exception Grpc_error of status
exception Transport_error of string

type conn = Plain of H2_lwt_unix.Client.t | Tls of H2_lwt_unix.Client.TLS.t

type t = {
  host : string;
  port : int;
  use_tls : bool;
  headers : (string * string) list;  (** extra headers sent with every request *)
  mutable conn : conn option;
  mutable connecting : conn Lwt.t option;
}

let create ?(headers = []) ?(use_tls = false) ~host ~port () =
  { host; port; use_tls; headers; conn = None; connecting = None }

let authority t = Printf.sprintf "%s:%d" t.host t.port
let transport_error fmt = Printf.ksprintf (fun s -> Lwt.fail (Transport_error s)) fmt

let describe_error : H2.Client_connection.error -> string = function
  | `Malformed_response s -> "malformed response: " ^ s
  | `Invalid_response_body_length _ -> "invalid response body length"
  | `Protocol_error (code, s) -> Printf.sprintf "protocol error %s: %s" (H2.Error_code.to_string code) s
  | `Exn e -> Printexc.to_string e

let resolve t =
  Lwt_unix.getaddrinfo t.host (string_of_int t.port) [ Unix.AI_SOCKTYPE Unix.SOCK_STREAM ] >>= function
  | [] -> transport_error "cannot resolve %s" t.host
  | ai :: _ -> Lwt.return ai

let open_socket t =
  resolve t >>= fun ai ->
  let fd = Lwt_unix.socket ai.Unix.ai_family ai.Unix.ai_socktype ai.Unix.ai_protocol in
  Lwt.catch
    (fun () -> Lwt_unix.connect fd ai.Unix.ai_addr >|= fun () -> fd)
    (fun e ->
      Lwt_unix.close fd >>= fun () -> transport_error "connect to %s failed: %s" (authority t) (Printexc.to_string e))

let connection_error_handler err =
  (* Connection-level errors surface per-stream too; log nothing here. *)
  ignore (describe_error err)

let connect t =
  open_socket t >>= fun fd ->
  if t.use_tls then begin
    let authenticator =
      match Ca_certs.authenticator () with
      | Ok a -> a
      | Error (`Msg m) -> raise (Transport_error ("no CA certificates: " ^ m))
    in
    let config =
      match Tls.Config.client ~authenticator ~alpn_protocols:[ "h2" ] () with
      | Ok c -> c
      | Error (`Msg m) -> raise (Transport_error ("TLS config: " ^ m))
    in
    let host = Result.to_option (Result.bind (Domain_name.of_string t.host) Domain_name.host) in
    Lwt.catch
      (fun () -> Tls_lwt.Unix.client_of_fd config ?host fd)
      (fun e ->
        Lwt_unix.close fd >>= fun () ->
        transport_error "TLS handshake with %s failed: %s" (authority t) (Printexc.to_string e))
    >>= fun tls ->
    H2_lwt_unix.Client.TLS.create_connection ~error_handler:connection_error_handler tls >|= fun c -> Tls c
  end
  else H2_lwt_unix.Client.create_connection ~error_handler:connection_error_handler fd >|= fun c -> Plain c

let is_closed = function Plain c -> H2_lwt_unix.Client.is_closed c | Tls c -> H2_lwt_unix.Client.TLS.is_closed c

let get_conn t =
  match t.conn with
  | Some c when not (is_closed c) -> Lwt.return c
  | _ -> (
      match t.connecting with
      | Some p -> p
      | None ->
          let p = connect t in
          t.connecting <- Some p;
          Lwt.finalize
            (fun () ->
              p >|= fun c ->
              t.conn <- Some c;
              c)
            (fun () ->
              t.connecting <- None;
              Lwt.return_unit))

let close t =
  match t.conn with
  | None -> Lwt.return_unit
  | Some c -> (
      t.conn <- None;
      match c with Plain c -> H2_lwt_unix.Client.shutdown c | Tls c -> H2_lwt_unix.Client.TLS.shutdown c)

(* ---- gRPC framing ---- *)

let frame payload =
  let n = String.length payload in
  let b = Bytes.create (5 + n) in
  Bytes.set b 0 '\000';
  Bytes.set_int32_be b 1 (Int32.of_int n);
  Bytes.blit_string payload 0 b 5 n;
  Bytes.unsafe_to_string b

(* Percent-decoding for grpc-message (RFC: %XX sequences). *)
let percent_decode s =
  let b = Buffer.create (String.length s) in
  let n = String.length s in
  let hex c =
    match c with
    | '0' .. '9' -> Char.code c - 48
    | 'a' .. 'f' -> Char.code c - 87
    | 'A' .. 'F' -> Char.code c - 55
    | _ -> -1
  in
  let rec go i =
    if i >= n then ()
    else if s.[i] = '%' && i + 2 < n && hex s.[i + 1] >= 0 && hex s.[i + 2] >= 0 then begin
      Buffer.add_char b (Char.chr ((hex s.[i + 1] * 16) + hex s.[i + 2]));
      go (i + 3)
    end
    else begin
      Buffer.add_char b s.[i];
      go (i + 1)
    end
  in
  go 0;
  Buffer.contents b

(* Base64 decoder tolerant of missing padding and URL-safe alphabets. *)
let base64_decode s =
  let dec c =
    match c with
    | 'A' .. 'Z' -> Char.code c - 65
    | 'a' .. 'z' -> Char.code c - 97 + 26
    | '0' .. '9' -> Char.code c - 48 + 52
    | '+' | '-' -> 62
    | '/' | '_' -> 63
    | _ -> -1
  in
  let b = Buffer.create (String.length s) in
  let acc = ref 0 and bits = ref 0 in
  String.iter
    (fun c ->
      let v = dec c in
      if v >= 0 then begin
        acc := (!acc lsl 6) lor v;
        bits := !bits + 6;
        if !bits >= 8 then begin
          bits := !bits - 8;
          Buffer.add_char b (Char.chr ((!acc lsr !bits) land 0xff))
        end
      end)
    s;
  Buffer.contents b

let status_of_headers (h : H2.Headers.t) =
  match H2.Headers.get h "grpc-status" with
  | None -> None
  | Some code ->
      let code = try int_of_string code with _ -> 2 in
      let message = Option.value ~default:"" (Option.map percent_decode (H2.Headers.get h "grpc-message")) in
      let details = Option.map base64_decode (H2.Headers.get h "grpc-status-details-bin") in
      Some { code; message; details }

(* Incremental splitter of the length-prefixed message stream. *)
type splitter = { buf : Buffer.t; mutable pending : int (* message length once header parsed, -1 otherwise *) }

let new_splitter () = { buf = Buffer.create 65536; pending = -1 }

let feed sp (chunk : string) emit =
  Buffer.add_string sp.buf chunk;
  let rec drain () =
    let have = Buffer.length sp.buf in
    if sp.pending < 0 && have >= 5 then begin
      let flag = Buffer.nth sp.buf 0 in
      if flag <> '\000' then raise (Transport_error "compressed gRPC messages are not supported");
      sp.pending <- Int32.to_int (String.get_int32_be (Buffer.sub sp.buf 0 5) 1);
      drain ()
    end
    else if sp.pending >= 0 && have >= 5 + sp.pending then begin
      let msg = Buffer.sub sp.buf 5 sp.pending in
      let rest = Buffer.sub sp.buf (5 + sp.pending) (have - 5 - sp.pending) in
      Buffer.clear sp.buf;
      Buffer.add_string sp.buf rest;
      sp.pending <- -1;
      emit msg;
      drain ()
    end
  in
  drain ()

(** Perform a call. [path] is "/pkg.Service/Method"; [payload] the encoded request. The result is a stream of raw
    response payloads; it ends when the server sends trailers, and fails with [Grpc_error] on a non-OK status or
    [Transport_error] on connection problems. Unary calls just read one element. *)
let call t ~path ~(payload : string) : string Lwt_stream.t Lwt.t =
  get_conn t >>= fun conn ->
  let stream, push = Lwt_stream.create () in
  let finished = ref false in
  let finish_ok () =
    if not !finished then (
      finished := true;
      push None)
  in
  (* Failures are delivered through a separate promise the consumer awaits. *)
  let err_t, err_u = Lwt.wait () in
  let fail exn =
    if not !finished then (
      finished := true;
      push None;
      Lwt.wakeup_later err_u (Some exn))
  in
  let got_eof = ref false and trailers = ref None in
  let maybe_done () =
    match (!got_eof, !trailers) with
    | true, Some st ->
        if st.code = 0 then (
          finish_ok ();
          Lwt.wakeup_later err_u None)
        else fail (Grpc_error st)
    | _ -> ()
  in
  let splitter = new_splitter () in
  let headers =
    H2.Headers.of_list
      ([
         (":authority", authority t);
         ("content-type", "application/grpc");
         ("te", "trailers");
         ("user-agent", "spark-connect-ocaml/0.1 h2");
       ]
      @ t.headers)
  in
  let request = H2.Request.create ~scheme:(if t.use_tls then "https" else "http") ~headers `POST path in
  let trailers_handler h =
    (match status_of_headers h with
    | Some st -> trailers := Some st
    | None -> trailers := Some { code = 2; message = "missing grpc-status in trailers"; details = None });
    maybe_done ()
  in
  let response_handler (resp : H2.Response.t) (body : H2.Body.Reader.t) =
    if H2.Status.to_code resp.status <> 200 then
      fail (Transport_error (Printf.sprintf "HTTP %d from %s" (H2.Status.to_code resp.status) path))
    else begin
      (* Trailers-only response: status in the initial headers. *)
      (match status_of_headers resp.headers with Some st -> trailers := Some st | None -> ());
      let rec loop () =
        H2.Body.Reader.schedule_read body
          ~on_eof:(fun () ->
            got_eof := true;
            maybe_done ())
          ~on_read:(fun bs ~off ~len ->
            (try feed splitter (Bigstringaf.substring bs ~off ~len) (fun m -> if not !finished then push (Some m))
             with e -> fail e);
            loop ())
      in
      loop ()
    end
  in
  let error_handler err = fail (Transport_error (describe_error err)) in
  let body =
    match conn with
    | Plain c -> H2_lwt_unix.Client.request c ~trailers_handler request ~error_handler ~response_handler
    | Tls c -> H2_lwt_unix.Client.TLS.request c ~trailers_handler request ~error_handler ~response_handler
  in
  H2.Body.Writer.write_string body (frame payload);
  H2.Body.Writer.close body;
  (* Wrap the stream so that a failure is raised to the consumer once the
     buffered elements are exhausted. *)
  let wrapped =
    Lwt_stream.from (fun () ->
        Lwt_stream.get stream >>= function
        | Some m -> Lwt.return (Some m)
        | None -> ( err_t >>= function None -> Lwt.return None | Some e -> Lwt.fail e))
  in
  Lwt.return wrapped

(** Unary convenience: exactly one response message. *)
let unary t ~path ~payload =
  call t ~path ~payload >>= fun s ->
  Lwt_stream.get s >>= function
  | Some m -> Lwt_stream.junk_while (fun _ -> true) s >|= fun () -> m
  | None -> Lwt.fail (Transport_error ("empty response from " ^ path))
