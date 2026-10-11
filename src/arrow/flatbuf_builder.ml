(* Minimal FlatBuffers builder (back-to-front, like the reference
   implementation) sufficient to emit Arrow IPC metadata. *)

type t = {
  mutable buf : Bytes.t;
  mutable space : int;
      (** index of the first used byte; data lives in the half-open range from space to the buffer length *)
  mutable minalign : int;
  mutable vtable : int array;  (** slot -> offset (from end) of the field, 0 if absent *)
  mutable nslots : int;
  mutable obj_start : int;
}

let create () = { buf = Bytes.make 1024 '\000'; space = 1024; minalign = 1; vtable = [||]; nslots = 0; obj_start = 0 }
let cap t = Bytes.length t.buf
let offset t = cap t - t.space

let grow t need =
  while t.space < need do
    let old = t.buf in
    let oldcap = Bytes.length old in
    let nb = Bytes.make (2 * oldcap) '\000' in
    Bytes.blit old 0 nb oldcap oldcap;
    t.buf <- nb;
    t.space <- t.space + oldcap
  done

(* Make room for [additional] bytes followed by a [size]-aligned value. *)
let prep t size additional =
  if size > t.minalign then t.minalign <- size;
  let align_size = (lnot (offset t + additional) + 1) land (size - 1) in
  grow t (align_size + size + additional);
  t.space <- t.space - align_size

let put_u8 t v =
  t.space <- t.space - 1;
  Bytes.set_uint8 t.buf t.space v

let put_i16 t v =
  t.space <- t.space - 2;
  Bytes.set_int16_le t.buf t.space v

let put_i32 t v =
  t.space <- t.space - 4;
  Bytes.set_int32_le t.buf t.space (Int32.of_int v)

let put_i64 t v =
  t.space <- t.space - 8;
  Bytes.set_int64_le t.buf t.space v

let add_u8 t v =
  prep t 1 0;
  put_u8 t v

let add_bool t b = add_u8 t (if b then 1 else 0)

let add_i16 t v =
  prep t 2 0;
  put_i16 t v

let add_i32 t v =
  prep t 4 0;
  put_i32 t v

let add_i64 t v =
  prep t 8 0;
  put_i64 t v

let add_uoffset t off =
  prep t 4 0;
  put_i32 t (offset t - off + 4)

let start_table t nslots =
  t.vtable <- Array.make nslots 0;
  t.nslots <- nslots;
  t.obj_start <- offset t

let slot t i = t.vtable.(i) <- offset t

let field_u8 t i v =
  add_u8 t v;
  slot t i

let field_bool t i v =
  add_bool t v;
  slot t i

let field_i16 t i v =
  add_i16 t v;
  slot t i

let field_i32 t i v =
  add_i32 t v;
  slot t i

let field_i64 t i v =
  add_i64 t v;
  slot t i

let field_uoffset t i off =
  add_uoffset t off;
  slot t i

let end_table t =
  add_i32 t 0;
  let obj_off = offset t in
  for i = t.nslots - 1 downto 0 do
    add_i16 t (if t.vtable.(i) = 0 then 0 else obj_off - t.vtable.(i))
  done;
  add_i16 t (obj_off - t.obj_start);
  add_i16 t ((t.nslots + 2) * 2);
  let vt_off = offset t in
  Bytes.set_int32_le t.buf (cap t - obj_off) (Int32.of_int (vt_off - obj_off));
  obj_off

let create_string t s =
  let n = String.length s in
  prep t 4 (n + 1);
  put_u8 t 0;
  t.space <- t.space - n;
  Bytes.blit_string s 0 t.buf t.space n;
  put_i32 t n;
  offset t

let start_vector t ~elem_size ~n ~alignment =
  prep t 4 (elem_size * n);
  prep t alignment (elem_size * n)

let end_vector t n =
  put_i32 t n;
  offset t

(* Vector of offsets to tables/strings created earlier. *)
let create_offset_vector t (offs : int list) =
  let n = List.length offs in
  start_vector t ~elem_size:4 ~n ~alignment:4;
  List.iter (fun o -> add_uoffset t o) (List.rev offs);
  end_vector t n

let finish t root =
  prep t t.minalign 4;
  add_uoffset t root;
  Bytes.sub_string t.buf t.space (offset t)
