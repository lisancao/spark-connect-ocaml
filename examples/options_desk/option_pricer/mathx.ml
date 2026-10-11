let pi = 4.0 *. atan 1.0
let inv_sqrt_2pi = 0.398942280401432677939946059934
let norm_pdf x = inv_sqrt_2pi *. exp (-0.5 *. x *. x)

(* Hart (1968) rational approximation to the standard normal CDF. Two regimes:
   a ratio of degree-6/degree-7 polynomials near the centre, and a continued
   fraction in the tails. Beyond |x| = 37 the result is 0 or 1 to within the
   smallest representable double, so we short-circuit. *)
let norm_cdf x =
  let a = Float.abs x in
  if a > 37.0
  then if x > 0.0 then 1.0 else 0.0
  else begin
    let e = exp (-0.5 *. a *. a) in
    let tail =
      if a < 7.07106781186547
      then begin
        let num = 3.52624965998911e-02 *. a +. 0.700383064443688 in
        let num = (num *. a) +. 6.37396220353165 in
        let num = (num *. a) +. 33.912866078383 in
        let num = (num *. a) +. 112.079291497871 in
        let num = (num *. a) +. 221.213596169931 in
        let num = (num *. a) +. 220.206867912376 in
        let den = 8.83883476483184e-02 *. a +. 1.75566716318264 in
        let den = (den *. a) +. 16.064177579207 in
        let den = (den *. a) +. 86.7807322029461 in
        let den = (den *. a) +. 296.564248779674 in
        let den = (den *. a) +. 637.333633378831 in
        let den = (den *. a) +. 793.826512519948 in
        let den = (den *. a) +. 440.413735824752 in
        e *. num /. den
      end
      else begin
        let b = a +. 0.65 in
        let b = a +. (4.0 /. b) in
        let b = a +. (3.0 /. b) in
        let b = a +. (2.0 /. b) in
        let b = a +. (1.0 /. b) in
        e /. (b *. 2.506628274631000502)
      end
    in
    (* [tail] is the upper-tail probability P(Z > |x|). *)
    if x > 0.0 then 1.0 -. tail else tail
  end
;;

let erf x = (2.0 *. norm_cdf (x *. sqrt 2.0)) -. 1.0
let erfc x = 2.0 *. norm_cdf (-.x *. sqrt 2.0)

(* Acklam's rational approximation (relative error < 1.15e-9) followed by one
   Halley step, which lifts it to full double precision. *)
let norm_inv_cdf p =
  if p <= 0.0
  then neg_infinity
  else if p >= 1.0
  then infinity
  else begin
    let a =
      [| -3.969683028665376e+01
       ; 2.209460984245205e+02
       ; -2.759285104469687e+02
       ; 1.383577518672690e+02
       ; -3.066479806614716e+01
       ; 2.506628277459239e+00
      |]
    in
    let b =
      [| -5.447609879822406e+01
       ; 1.615858368580409e+02
       ; -1.556989798598866e+02
       ; 6.680131188771972e+01
       ; -1.328068155288572e+01
      |]
    in
    let c =
      [| -7.784894002430293e-03
       ; -3.223964580411365e-01
       ; -2.400758277161838e+00
       ; -2.549732539343734e+00
       ; 4.374664141464968e+00
       ; 2.938163982698783e+00
      |]
    in
    let d =
      [| 7.784695709041462e-03
       ; 3.224671290700398e-01
       ; 2.445134137142996e+00
       ; 3.754408661907416e+00
      |]
    in
    let p_low = 0.02425 in
    let p_high = 1.0 -. p_low in
    let x =
      if p < p_low
      then begin
        let q = sqrt (-2.0 *. log p) in
        let num =
          ((((((((c.(0) *. q) +. c.(1)) *. q) +. c.(2)) *. q) +. c.(3)) *. q) +. c.(4))
          *. q
          +. c.(5)
        in
        let den =
          ((((((d.(0) *. q) +. d.(1)) *. q) +. d.(2)) *. q) +. d.(3)) *. q +. 1.0
        in
        num /. den
      end
      else if p <= p_high
      then begin
        let q = p -. 0.5 in
        let r = q *. q in
        let num =
          ((((((((a.(0) *. r) +. a.(1)) *. r) +. a.(2)) *. r) +. a.(3)) *. r) +. a.(4))
          *. r
          +. a.(5)
        in
        let den =
          ((((((((b.(0) *. r) +. b.(1)) *. r) +. b.(2)) *. r) +. b.(3)) *. r) +. b.(4))
          *. r
          +. 1.0
        in
        q *. num /. den
      end
      else begin
        let q = sqrt (-2.0 *. log (1.0 -. p)) in
        let num =
          ((((((((c.(0) *. q) +. c.(1)) *. q) +. c.(2)) *. q) +. c.(3)) *. q) +. c.(4))
          *. q
          +. c.(5)
        in
        let den =
          ((((((d.(0) *. q) +. d.(1)) *. q) +. d.(2)) *. q) +. d.(3)) *. q +. 1.0
        in
        -.(num /. den)
      end
    in
    (* Halley refinement on f(x) = Phi(x) - p. *)
    let e = norm_cdf x -. p in
    let u = e /. norm_pdf x in
    x -. (u /. (1.0 +. (x *. u /. 2.0)))
  end
;;

let bisect ?(tol = 1e-12) ?(max_iter = 200) f lo hi =
  let flo = f lo
  and fhi = f hi in
  if Float.abs flo <= tol
  then Ok lo
  else if Float.abs fhi <= tol
  then Ok hi
  else if flo *. fhi > 0.0
  then
    Error
      (Printf.sprintf
         "bisect: root not bracketed on [%g, %g] (f = %g, %g)"
         lo
         hi
         flo
         fhi)
  else begin
    let lo = ref lo
    and hi = ref hi
    and flo = ref flo in
    let result = ref None in
    let i = ref 0 in
    while !result = None && !i < max_iter do
      let mid = 0.5 *. (!lo +. !hi) in
      let fmid = f mid in
      if Float.abs fmid <= tol || (!hi -. !lo) /. 2.0 <= tol
      then result := Some mid
      else begin
        if !flo *. fmid < 0.0
        then hi := mid
        else begin
          lo := mid;
          flo := fmid
        end;
        incr i
      end
    done;
    match !result with
    | Some x -> Ok x
    | None -> Ok (0.5 *. (!lo +. !hi))
  end
;;

let newton_bracketed ?(tol = 1e-12) ?(max_iter = 100) ~f ~df ~lo ~hi x0 =
  let flo = f lo
  and fhi = f hi in
  if Float.abs flo <= tol
  then Ok lo
  else if Float.abs fhi <= tol
  then Ok hi
  else if flo *. fhi > 0.0
  then
    Error
      (Printf.sprintf
         "newton: root not bracketed on [%g, %g] (f = %g, %g)"
         lo
         hi
         flo
         fhi)
  else begin
    (* Orient the bracket so that f is negative at [a] and positive at [b]. *)
    let a = ref (if flo < 0.0 then lo else hi) in
    let b = ref (if flo < 0.0 then hi else lo) in
    let x = ref (if x0 > Float.min lo hi && x0 < Float.max lo hi then x0 else 0.5 *. (lo +. hi)) in
    let answer = ref None in
    let i = ref 0 in
    while !answer = None && !i < max_iter do
      let fx = f !x in
      if Float.abs fx <= tol
      then answer := Some !x
      else begin
        if fx < 0.0 then a := !x else b := !x;
        let dfx = df !x in
        let candidate = if Float.abs dfx > 1e-14 then !x -. (fx /. dfx) else nan in
        let inside =
          Float.is_finite candidate
          && candidate > Float.min !a !b
          && candidate < Float.max !a !b
        in
        let next = if inside then candidate else 0.5 *. (!a +. !b) in
        if Float.abs (next -. !x) <= tol
        then begin
          x := next;
          answer := Some next
        end
        else x := next;
        incr i
      end
    done;
    match !answer with
    | Some v -> Ok v
    | None -> Ok !x
  end
;;
