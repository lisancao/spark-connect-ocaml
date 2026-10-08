(* Scalar and nested values decoded from Arrow record batches. *)

type time_unit = Second | Millisecond | Microsecond | Nanosecond

type t =
  | Null
  | Bool of bool
  | Int of int  (** 8-, 16- and 32-bit integers, signed or unsigned *)
  | Int64 of int64  (** 64-bit integers; unsigned values keep their bit pattern *)
  | Float of float
  | String of string
  | Binary of string
  | Decimal of string  (** rendered in decimal notation, e.g. ["-12.340"] *)
  | Date of int  (** days since 1970-01-01 *)
  | Time of int64 * time_unit  (** time of day since midnight *)
  | Timestamp of int64 * time_unit * string option  (** value, unit, timezone *)
  | Duration of int64 * time_unit
  | Interval_year_month of int  (** months *)
  | Interval_day_time of int * int  (** days, milliseconds *)
  | Interval_month_day_nano of int * int * int64
  | List of t array
  | Struct of (string * t) array
  | Map of (t * t) array

let unit_to_string = function Second -> "s" | Millisecond -> "ms" | Microsecond -> "us" | Nanosecond -> "ns"

let unit_per_second = function
  | Second -> 1L
  | Millisecond -> 1_000L
  | Microsecond -> 1_000_000L
  | Nanosecond -> 1_000_000_000L

(* Howard Hinnant's civil-from-days, valid for the proleptic Gregorian calendar. *)
let civil_of_days z =
  let z = z + 719468 in
  let era = (if z >= 0 then z else z - 146096) / 146097 in
  let doe = z - (era * 146097) in
  let yoe = (doe - (doe / 1460) + (doe / 36524) - (doe / 146096)) / 365 in
  let y = yoe + (era * 400) in
  let doy = doe - ((365 * yoe) + (yoe / 4) - (yoe / 100)) in
  let mp = ((5 * doy) + 2) / 153 in
  let d = doy - (((153 * mp) + 2) / 5) + 1 in
  let m = if mp < 10 then mp + 3 else mp - 9 in
  ((if m <= 2 then y + 1 else y), m, d)

let days_of_civil y m d =
  let y = if m <= 2 then y - 1 else y in
  let era = (if y >= 0 then y else y - 399) / 400 in
  let yoe = y - (era * 400) in
  let doy = (((153 * if m > 2 then m - 3 else m + 9) + 2) / 5) + d - 1 in
  let doe = (yoe * 365) + (yoe / 4) - (yoe / 100) + doy in
  (era * 146097) + doe - 719468

let date_to_string days =
  let y, m, d = civil_of_days days in
  Printf.sprintf "%04d-%02d-%02d" y m d

(* Floor division / modulo for int64 (OCaml's are truncating). *)
let fdiv a b =
  let q = Int64.div a b in
  if Int64.rem a b < 0L && b > 0L then Int64.pred q else q

let fmod a b = Int64.sub a (Int64.mul (fdiv a b) b)

let timestamp_to_string v unit =
  let per_s = unit_per_second unit in
  let secs = fdiv v per_s and frac = fmod v per_s in
  let days = Int64.to_int (fdiv secs 86400L) and sod = Int64.to_int (fmod secs 86400L) in
  let y, m, d = civil_of_days days in
  let base = Printf.sprintf "%04d-%02d-%02d %02d:%02d:%02d" y m d (sod / 3600) (sod / 60 mod 60) (sod mod 60) in
  match unit with
  | Second -> base
  | Millisecond -> Printf.sprintf "%s.%03Ld" base frac
  | Microsecond -> Printf.sprintf "%s.%06Ld" base frac
  | Nanosecond -> Printf.sprintf "%s.%09Ld" base frac

let time_to_string v unit =
  let per_s = unit_per_second unit in
  let secs = Int64.to_int (fdiv v per_s) and frac = fmod v per_s in
  let base = Printf.sprintf "%02d:%02d:%02d" (secs / 3600) (secs / 60 mod 60) (secs mod 60) in
  match unit with
  | Second -> base
  | Millisecond -> Printf.sprintf "%s.%03Ld" base frac
  | Microsecond -> Printf.sprintf "%s.%06Ld" base frac
  | Nanosecond -> Printf.sprintf "%s.%09Ld" base frac

let rec to_string = function
  | Null -> "NULL"
  | Bool b -> string_of_bool b
  | Int i -> string_of_int i
  | Int64 i -> Int64.to_string i
  | Float f -> if Float.is_integer f && Float.abs f < 1e15 then Printf.sprintf "%.1f" f else Printf.sprintf "%.17g" f
  | String s -> s
  | Binary b ->
      Printf.sprintf "0x%s"
        (String.concat "" (List.init (String.length b) (fun i -> Printf.sprintf "%02X" (Char.code b.[i]))))
  | Decimal s -> s
  | Date d -> date_to_string d
  | Time (v, u) -> time_to_string v u
  | Timestamp (v, u, _) -> timestamp_to_string v u
  | Duration (v, u) -> Printf.sprintf "%Ld%s" v (unit_to_string u)
  | Interval_year_month m -> Printf.sprintf "INTERVAL '%d-%d' YEAR TO MONTH" (m / 12) (abs m mod 12)
  | Interval_day_time (d, ms) -> Printf.sprintf "INTERVAL %d days %d ms" d ms
  | Interval_month_day_nano (m, d, ns) -> Printf.sprintf "INTERVAL %d months %d days %Ld ns" m d ns
  | List vs -> "[" ^ String.concat ", " (Array.to_list (Array.map to_string vs)) ^ "]"
  | Struct fs -> "{" ^ String.concat ", " (Array.to_list (Array.map (fun (k, v) -> k ^ ": " ^ to_string v) fs)) ^ "}"
  | Map kvs ->
      "{" ^ String.concat ", " (Array.to_list (Array.map (fun (k, v) -> to_string k ^ " -> " ^ to_string v) kvs)) ^ "}"

let rec pp fmt v =
  match v with
  | String s -> Format.fprintf fmt "%S" s
  | List vs ->
      Format.fprintf fmt "[@[<hov>%a@]]"
        (Format.pp_print_list ~pp_sep:(fun f () -> Format.fprintf f ",@ ") pp)
        (Array.to_list vs)
  | Struct fs ->
      Format.fprintf fmt "{@[<hov>%a@]}"
        (Format.pp_print_list
           ~pp_sep:(fun f () -> Format.fprintf f ",@ ")
           (fun f (k, v) -> Format.fprintf f "%s = %a" k pp v))
        (Array.to_list fs)
  | Map kvs ->
      Format.fprintf fmt "{@[<hov>%a@]}"
        (Format.pp_print_list
           ~pp_sep:(fun f () -> Format.fprintf f ",@ ")
           (fun f (k, v) -> Format.fprintf f "%a -> %a" pp k pp v))
        (Array.to_list kvs)
  | v -> Format.pp_print_string fmt (to_string v)

(* Convenience accessors; each raises [Invalid_argument] on a type mismatch. *)
let mismatch what v = invalid_arg (Printf.sprintf "Value.%s: got %s" what (to_string v))
let to_int = function Int i -> i | Int64 i -> Int64.to_int i | v -> mismatch "to_int" v
let to_int64 = function Int i -> Int64.of_int i | Int64 i -> i | v -> mismatch "to_int64" v

let to_float = function
  | Float f -> f
  | Int i -> float_of_int i
  | Int64 i -> Int64.to_float i
  | v -> mismatch "to_float" v

let to_bool = function Bool b -> b | v -> mismatch "to_bool" v
let to_str = function String s -> s | v -> mismatch "to_str" v
let is_null = function Null -> true | _ -> false
let to_int_opt = function Null -> None | v -> Some (to_int v)
let to_int64_opt = function Null -> None | v -> Some (to_int64 v)
let to_float_opt = function Null -> None | v -> Some (to_float v)
let to_bool_opt = function Null -> None | v -> Some (to_bool v)
let to_str_opt = function Null -> None | v -> Some (to_str v)

(* Decimal rendering: little-endian two's-complement integer of [n] bytes
   with [scale] fractional digits. Pure OCaml, no bignum dependency. *)
let decimal_of_le_bytes (s : string) ~off ~len ~scale =
  let neg = Char.code s.[off + len - 1] land 0x80 <> 0 in
  (* magnitude as big-endian byte array *)
  let mag = Bytes.create len in
  for i = 0 to len - 1 do
    let b = Char.code s.[off + len - 1 - i] in
    Bytes.set mag i (Char.chr (if neg then lnot b land 0xff else b))
  done;
  if neg then begin
    (* add one *)
    let carry = ref 1 in
    for i = len - 1 downto 0 do
      if !carry > 0 then begin
        let v = Char.code (Bytes.get mag i) + !carry in
        Bytes.set mag i (Char.chr (v land 0xff));
        carry := v lsr 8
      end
    done
  end;
  (* repeated division by 10^9 of the big-endian magnitude *)
  let limbs = ref [] in
  let cur = Bytes.copy mag in
  let is_zero b =
    let z = ref true in
    Bytes.iter (fun c -> if c <> '\000' then z := false) b;
    !z
  in
  while not (is_zero cur) do
    let rem = ref 0 in
    for i = 0 to len - 1 do
      let v = (!rem lsl 8) lor Char.code (Bytes.get cur i) in
      Bytes.set cur i (Char.chr (v / 1_000_000_000));
      rem := v mod 1_000_000_000
    done;
    limbs := !rem :: !limbs
  done;
  let digits =
    match !limbs with
    | [] -> "0"
    | hi :: rest -> string_of_int hi ^ String.concat "" (List.map (Printf.sprintf "%09d") rest)
  in
  let digits =
    if scale <= 0 then digits ^ String.make (-scale) '0'
    else begin
      let digits =
        if String.length digits <= scale then String.make (scale - String.length digits + 1) '0' ^ digits else digits
      in
      let n = String.length digits in
      String.sub digits 0 (n - scale) ^ "." ^ String.sub digits (n - scale) scale
    end
  in
  if neg && digits <> "0" then "-" ^ digits else digits
