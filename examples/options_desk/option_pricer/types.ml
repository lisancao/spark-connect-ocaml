type option_type =
  | Call
  | Put

type exercise =
  | European
  | American

type contract =
  { kind : option_type
  ; style : exercise
  }

type market =
  { spot : float
  ; strike : float
  ; ttm : float
  ; rate : float
  ; div_yield : float
  ; vol : float
  }

let european kind = { kind; style = European }
let american kind = { kind; style = American }

let string_of_option_type = function
  | Call -> "call"
  | Put -> "put"
;;

let string_of_exercise = function
  | European -> "european"
  | American -> "american"
;;

let option_type_of_string s =
  match String.lowercase_ascii (String.trim s) with
  | "call" | "c" -> Ok Call
  | "put" | "p" -> Ok Put
  | other -> Error (Printf.sprintf "unrecognised option type %S" other)
;;

let exercise_of_string s =
  match String.lowercase_ascii (String.trim s) with
  | "european" | "euro" | "eu" | "e" -> Ok European
  | "american" | "amer" | "am" | "a" -> Ok American
  | other -> Error (Printf.sprintf "unrecognised exercise style %S" other)
;;

let intrinsic ~kind ~spot ~strike =
  match kind with
  | Call -> Float.max (spot -. strike) 0.
  | Put -> Float.max (strike -. spot) 0.
;;

let payoff kind m s = intrinsic ~kind ~spot:s ~strike:m.strike

let validate m =
  let finite name x =
    if Float.is_finite x then Ok () else Error (Printf.sprintf "%s is not finite" name)
  in
  let non_negative name x =
    if x >= 0. then Ok () else Error (Printf.sprintf "%s must be >= 0 (got %g)" name x)
  in
  let checks =
    [ finite "spot" m.spot
    ; finite "strike" m.strike
    ; finite "ttm" m.ttm
    ; finite "rate" m.rate
    ; finite "div_yield" m.div_yield
    ; finite "vol" m.vol
    ; non_negative "spot" m.spot
    ; non_negative "strike" m.strike
    ; non_negative "ttm" m.ttm
    ; non_negative "vol" m.vol
    ]
  in
  List.fold_left
    (fun acc c ->
      match acc with
      | Error _ -> acc
      | Ok () -> c)
    (Ok ())
    checks
;;

let describe c m =
  Printf.sprintf
    "%s %s S=%.2f K=%.2f T=%.4fy r=%.2f%% q=%.2f%% vol=%.2f%%"
    (string_of_exercise c.style)
    (string_of_option_type c.kind)
    m.spot
    m.strike
    m.ttm
    (m.rate *. 100.)
    (m.div_yield *. 100.)
    (m.vol *. 100.)
;;
