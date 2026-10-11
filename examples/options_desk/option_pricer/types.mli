(** Core domain types shared by every pricing model.

    The point of this module is that the *contract* and the *market* are
    separate things. A model that can only handle European exercise takes an
    {!option_type}; a model that understands early exercise takes a full
    {!contract}. That makes "can this model price this payoff?" a question the
    type checker answers rather than a runtime check. *)

(** Payoff direction at expiry. *)
type option_type =
  | Call
  | Put

(** When the holder is allowed to exercise. *)
type exercise =
  | European  (** exercise only at expiry *)
  | American  (** exercise at any time up to expiry *)

(** A contract is a payoff plus an exercise style. *)
type contract =
  { kind : option_type
  ; style : exercise
  }

(** Market inputs, all continuously compounded and in absolute (not
    percentage) units: a 5% rate is [0.05], a 20% vol is [0.20]. *)
type market =
  { spot : float  (** current underlying price, [S] *)
  ; strike : float  (** strike, [K] *)
  ; ttm : float  (** time to maturity in years, [T] *)
  ; rate : float  (** continuously compounded risk-free rate, [r] *)
  ; div_yield : float  (** continuous dividend yield, [q] *)
  ; vol : float  (** annualised volatility, [sigma] *)
  }

val european : option_type -> contract
val american : option_type -> contract

val string_of_option_type : option_type -> string
val string_of_exercise : exercise -> string

(** Parsers accept a few common spellings ("call"/"c", "american"/"amer"/"a",
    case-insensitive) and return [Error msg] on anything else. *)

val option_type_of_string : string -> (option_type, string) result
val exercise_of_string : string -> (exercise, string) result

(** [intrinsic ~kind ~spot ~strike] is the immediate-exercise value,
    [max (S - K) 0] for a call and [max (K - S) 0] for a put. *)
val intrinsic : kind:option_type -> spot:float -> strike:float -> float

(** [payoff kind market s] is the terminal payoff at underlying price [s],
    using [market.strike]. *)
val payoff : option_type -> market -> float -> float

(** Rejects non-finite inputs, negative spot/strike/vol and negative time to
    maturity. Zero [ttm] and zero [vol] are allowed: every model degrades to
    the appropriate deterministic limit. *)
val validate : market -> (unit, string) result

(** A one-line human-readable description, e.g. ["american put S=100.00
    K=105.00 T=0.50y r=4.00% q=1.00% vol=25.00%"]. *)
val describe : contract -> market -> string
