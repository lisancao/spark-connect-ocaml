# Vendored: option_pricer (Black-Scholes subset)

`types.ml`, `black_scholes.ml` and `mathx.ml` (with their interfaces) are copied
unmodified from Case Ollila's
[option-pricing-model-comparison](https://github.com/crollila/option-pricing-model-comparison)
at commit `c181f4d7e0d0ca47bff140b5113cc759b801f721`, under the MIT license in
[`LICENSE`](LICENSE). Only the closed-form Black-Scholes pricer and its math helpers
are included; the binomial, Monte Carlo and implied-volatility modules are not.
