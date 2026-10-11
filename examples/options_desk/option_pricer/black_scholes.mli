(** Closed-form Black-Scholes-Merton pricing with a continuous dividend yield.

    Note the signature: these functions take an {!Types.option_type}, not a
    {!Types.contract}. Black-Scholes has no notion of early exercise, so there
    is deliberately no way to hand it an American contract. *)

open Types

(** Greeks in raw analytic units:
    - [delta]: d(price)/dS, dimensionless
    - [gamma]: d2(price)/dS2, per unit of spot
    - [vega]: d(price)/d(sigma) for a change of 1.00 (i.e. 100 vol points)
    - [theta]: d(price)/dt per year (negative for most long options)
    - [rho]: d(price)/dr for a change of 1.00 (i.e. 10000bp)

    {!vega_per_point}, {!theta_per_day} and {!rho_per_bp} convert to the units
    a trader would quote. *)
type greeks =
  { delta : float
  ; gamma : float
  ; vega : float
  ; theta : float
  ; rho : float
  }

(** [d1_d2 m] returns [None] when the diffusion term [sigma * sqrt T] vanishes,
    in which case the option is a deterministic forward payoff. *)
val d1_d2 : market -> (float * float) option

(** European price. Degenerate inputs are handled in the limit: [ttm = 0]
    gives the intrinsic value, and zero volatility gives the discounted
    forward intrinsic. *)
val price : option_type -> market -> float

val delta : option_type -> market -> float
val gamma : option_type -> market -> float
val vega : option_type -> market -> float
val theta : option_type -> market -> float
val rho : option_type -> market -> float
val greeks : option_type -> market -> greeks

val vega_per_point : float -> float
val theta_per_day : float -> float
val rho_per_bp : float -> float

(** [parity_gap m] is
    [call - put - (S * exp (-qT) - K * exp (-rT))], which is zero for
    arbitrage-free European prices. Used as a self-check in the test suite and
    reported by the CLI. *)
val parity_gap : market -> float
