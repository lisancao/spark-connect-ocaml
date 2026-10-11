(** Special functions and root-finding, implemented here rather than pulled
    from a library so that the accuracy of the pricing results is entirely
    attributable to code in this repository. *)

val pi : float

(** Standard normal density. *)
val norm_pdf : float -> float

(** Standard normal CDF via Hart's (1968) rational approximation, in the
    formulation popularised by Graeme West. Accurate to roughly double
    precision (|error| < 1e-15) across the whole real line, which is what
    lets the Black-Scholes tests assert to 1e-6 on prices without the CDF
    being the binding error term. *)
val norm_cdf : float -> float

(** Inverse standard normal CDF: Acklam's rational approximation followed by
    one Halley refinement step, giving close to full double precision. Defined
    on (0, 1); returns [neg_infinity] / [infinity] at the endpoints. *)
val norm_inv_cdf : float -> float

(** Error function, derived from {!norm_cdf} via
    [erf x = 2 * Phi (x * sqrt 2) - 1]. *)
val erf : float -> float

val erfc : float -> float

(** [bisect ?tol ?max_iter f lo hi] finds a root of [f] on a bracket where
    [f lo] and [f hi] have opposite signs. Returns [Error] if the bracket is
    invalid. *)
val bisect
  :  ?tol:float
  -> ?max_iter:int
  -> (float -> float)
  -> float
  -> float
  -> (float, string) result

(** [newton_bracketed ?tol ?max_iter ~f ~df ~lo ~hi x0] is Newton's method
    with a bisection safeguard: a Newton step that leaves the bracket or fails
    to make progress is replaced by a bisection step. This keeps the quadratic
    convergence of Newton where the derivative is well behaved (at-the-money
    implied vol) while retaining the guaranteed convergence of bisection deep
    in the wings, where vega collapses. *)
val newton_bracketed
  :  ?tol:float
  -> ?max_iter:int
  -> f:(float -> float)
  -> df:(float -> float)
  -> lo:float
  -> hi:float
  -> float
  -> (float, string) result
