(* Minimal FlatBuffers reader.

   Just enough of the FlatBuffers binary format to walk Arrow IPC metadata
   (Message, Schema, Field, RecordBatch). Everything is read directly from an
   immutable [string]; there is no object model. *)

exception Malformed of string

let fail fmt = Printf.ksprintf (fun s -> raise (Malformed s)) fmt
let u8 s p = Char.code (String.unsafe_get s p)
let u16 s p = String.get_uint16_le s p
let i16 s p = String.get_int16_le s p
let i32 s p = Int32.to_int (String.get_int32_le s p)
let u32 s p = Int32.to_int (String.get_int32_le s p) land 0xFFFF_FFFF
let i64 s p = String.get_int64_le s p
let check s p n = if p < 0 || p + n > String.length s then fail "offset %d+%d out of bounds (%d)" p n (String.length s)

type table = { s : string; pos : int }
(** A table is a position inside a buffer. *)

(** The root table of a FlatBuffer. *)
let root s =
  check s 0 4;
  let pos = u32 s 0 in
  check s pos 4;
  { s; pos }

(* Position of the vtable of [t]: the table starts with a signed offset to it. *)
let vtable t =
  let vt = t.pos - i32 t.s t.pos in
  check t.s vt 4;
  vt

(* Absolute position of field [id] in table [t], if present. *)
let field_pos t id =
  let vt = vtable t in
  let vsize = u16 t.s vt in
  let o = 4 + (2 * id) in
  if o + 2 > vsize then None
  else
    let v = u16 t.s (vt + o) in
    if v = 0 then None else Some (t.pos + v)

(* Follow a uoffset stored at [p]. *)
let deref s p =
  check s p 4;
  let q = p + u32 s p in
  check s q 4;
  q

let get_bool ?(default = false) t id =
  match field_pos t id with
  | None -> default
  | Some p ->
      check t.s p 1;
      u8 t.s p <> 0

let get_u8 ?(default = 0) t id =
  match field_pos t id with
  | None -> default
  | Some p ->
      check t.s p 1;
      u8 t.s p

let get_i16 ?(default = 0) t id =
  match field_pos t id with
  | None -> default
  | Some p ->
      check t.s p 2;
      i16 t.s p

let get_i32 ?(default = 0) t id =
  match field_pos t id with
  | None -> default
  | Some p ->
      check t.s p 4;
      i32 t.s p

let get_i64 ?(default = 0L) t id =
  match field_pos t id with
  | None -> default
  | Some p ->
      check t.s p 8;
      i64 t.s p

let get_table t id = match field_pos t id with None -> None | Some p -> Some { s = t.s; pos = deref t.s p }

let get_string t id =
  match field_pos t id with
  | None -> None
  | Some p ->
      let q = deref t.s p in
      let len = u32 t.s q in
      check t.s (q + 4) len;
      Some (String.sub t.s (q + 4) len)

(* A vector: position of first element and element count. *)
let get_vector t id =
  match field_pos t id with
  | None -> None
  | Some p ->
      let q = deref t.s p in
      let len = u32 t.s q in
      Some (q + 4, len)

(** Vector of tables: each element is a uoffset relative to itself. *)
let get_table_vector t id =
  match get_vector t id with
  | None -> []
  | Some (start, len) ->
      check t.s start (4 * len);
      List.init len (fun i -> { s = t.s; pos = deref t.s (start + (4 * i)) })

(** Vector of inline structs of [size] bytes: absolute positions of each element. *)
let get_struct_vector t id ~size =
  match get_vector t id with
  | None -> []
  | Some (start, len) ->
      check t.s start (size * len);
      List.init len (fun i -> start + (size * i))
