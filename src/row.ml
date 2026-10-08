(* A row of a collected result: column names plus values in column order. *)

type t = { names : string array; values : Value.t array }

let names r = r.names
let values r = r.values
let length r = Array.length r.values
let get r i = r.values.(i)

let index r name =
  let rec go i = if i >= Array.length r.names then raise Not_found else if r.names.(i) = name then i else go (i + 1) in
  go 0

let field r name = r.values.(index r name)
let field_opt r name = match index r name with i -> Some r.values.(i) | exception Not_found -> None
let to_list r = Array.to_list r.values
let to_alist r = Array.to_list (Array.mapi (fun i v -> (r.names.(i), v)) r.values)
let to_string r = "[" ^ String.concat ", " (Array.to_list (Array.map Value.to_string r.values)) ^ "]"

let pp fmt r =
  Format.fprintf fmt "[@[<hov>%a@]]"
    (Format.pp_print_list
       ~pp_sep:(fun f () -> Format.fprintf f ",@ ")
       (fun f (k, v) -> Format.fprintf f "%s = %a" k Value.pp v))
    (to_alist r)
