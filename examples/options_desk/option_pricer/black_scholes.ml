open Types

type greeks =
  { delta : float
  ; gamma : float
  ; vega : float
  ; theta : float
  ; rho : float
  }

let df_r m = exp (-.m.rate *. m.ttm)
let df_q m = exp (-.m.div_yield *. m.ttm)
let sqrt_t m = sqrt m.ttm
let sd m = m.vol *. sqrt_t m

let d1_d2 m =
  let s = sd m in
  if m.ttm <= 0. || s <= 1e-14 || m.spot <= 0. || m.strike <= 0.
  then None
  else begin
    let d1 =
      (log (m.spot /. m.strike)
       +. ((m.rate -. m.div_yield +. (0.5 *. m.vol *. m.vol)) *. m.ttm))
      /. s
    in
    Some (d1, d1 -. s)
  end
;;

(* The zero-vol / zero-time limit: the payoff becomes deterministic, worth the
   discounted intrinsic value of the forward. *)
let degenerate_price kind m =
  if m.ttm <= 0.
  then intrinsic ~kind ~spot:m.spot ~strike:m.strike
  else begin
    let fwd = m.spot *. df_q m
    and k = m.strike *. df_r m in
    match kind with
    | Call -> Float.max (fwd -. k) 0.
    | Put -> Float.max (k -. fwd) 0.
  end
;;

let price kind m =
  match d1_d2 m with
  | None -> degenerate_price kind m
  | Some (d1, d2) ->
    let sq = m.spot *. df_q m
    and kr = m.strike *. df_r m in
    (match kind with
     | Call -> (sq *. Mathx.norm_cdf d1) -. (kr *. Mathx.norm_cdf d2)
     | Put -> (kr *. Mathx.norm_cdf (-.d2)) -. (sq *. Mathx.norm_cdf (-.d1)))
;;

let delta kind m =
  match d1_d2 m with
  | None ->
    (* Deterministic limit: the option is worth a forward if in the money. *)
    let itm =
      match kind with
      | Call -> m.spot *. df_q m > m.strike *. df_r m
      | Put -> m.strike *. df_r m > m.spot *. df_q m
    in
    if not itm
    then 0.
    else (
      match kind with
      | Call -> df_q m
      | Put -> -.df_q m)
  | Some (d1, _) ->
    (match kind with
     | Call -> df_q m *. Mathx.norm_cdf d1
     | Put -> -.(df_q m *. Mathx.norm_cdf (-.d1)))
;;

(* Gamma and vega do not depend on the call/put flag: the two differ by a
   forward contract, which is linear in spot and independent of vol. *)
let gamma _kind m =
  match d1_d2 m with
  | None -> 0.
  | Some (d1, _) -> df_q m *. Mathx.norm_pdf d1 /. (m.spot *. sd m)
;;

let vega _kind m =
  match d1_d2 m with
  | None -> 0.
  | Some (d1, _) -> m.spot *. df_q m *. Mathx.norm_pdf d1 *. sqrt_t m
;;

let theta kind m =
  match d1_d2 m with
  | None -> 0.
  | Some (d1, d2) ->
    let sq = m.spot *. df_q m
    and kr = m.strike *. df_r m in
    let decay = -.(sq *. Mathx.norm_pdf d1 *. m.vol /. (2.0 *. sqrt_t m)) in
    (match kind with
     | Call ->
       decay
       -. (m.rate *. kr *. Mathx.norm_cdf d2)
       +. (m.div_yield *. sq *. Mathx.norm_cdf d1)
     | Put ->
       decay
       +. (m.rate *. kr *. Mathx.norm_cdf (-.d2))
       -. (m.div_yield *. sq *. Mathx.norm_cdf (-.d1)))
;;

let rho kind m =
  match d1_d2 m with
  | None -> 0.
  | Some (_, d2) ->
    let kr = m.strike *. df_r m in
    (match kind with
     | Call -> m.ttm *. kr *. Mathx.norm_cdf d2
     | Put -> -.(m.ttm *. kr *. Mathx.norm_cdf (-.d2)))
;;

let greeks kind m =
  { delta = delta kind m
  ; gamma = gamma kind m
  ; vega = vega kind m
  ; theta = theta kind m
  ; rho = rho kind m
  }
;;

let vega_per_point v = v /. 100.
let theta_per_day t = t /. 365.
let rho_per_bp r = r /. 10000.

let parity_gap m =
  let c = price Call m
  and p = price Put m in
  c -. p -. ((m.spot *. df_q m) -. (m.strike *. df_r m))
;;
