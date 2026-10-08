(* Spark SQL functions as column builders. Anything not listed is one call
   away through [Column.fn] / [call]. *)

open Column

let call = fn
let col = col
let lit = lit
let expr = expr

(* aggregates *)
let count c = fn "count" [ c ]
let count_all () = fn "count" [ int 1 ]
let count_distinct cs = fn ~distinct:true "count" cs
let sum c = fn "sum" [ c ]
let sum_distinct c = fn ~distinct:true "sum" [ c ]
let avg c = fn "avg" [ c ]
let mean = avg
let max c = fn "max" [ c ]
let min c = fn "min" [ c ]
let first ?(ignore_nulls = false) c = fn "first" [ c; bool ignore_nulls ]
let last ?(ignore_nulls = false) c = fn "last" [ c; bool ignore_nulls ]
let collect_list c = fn "collect_list" [ c ]
let collect_set c = fn "collect_set" [ c ]
let approx_count_distinct c = fn "approx_count_distinct" [ c ]
let stddev c = fn "stddev" [ c ]
let variance c = fn "variance" [ c ]
let percentile_approx c p = fn "percentile_approx" [ c; p ]
let max_by c ord = fn "max_by" [ c; ord ]
let min_by c ord = fn "min_by" [ c; ord ]

(* math *)
let abs c = fn "abs" [ c ]
let sqrt c = fn "sqrt" [ c ]
let pow a b = fn "power" [ a; b ]
let exp c = fn "exp" [ c ]
let log c = fn "ln" [ c ]
let log10 c = fn "log10" [ c ]
let round ?(scale = 0) c = fn "round" [ c; int scale ]
let floor c = fn "floor" [ c ]
let ceil c = fn "ceil" [ c ]
let rand () = fn "rand" []
let rand_seed s = fn "rand" [ int64 s ]
let randn () = fn "randn" []
let greatest cs = fn "greatest" cs
let least cs = fn "least" cs
let hash cs = fn "hash" cs

(* strings *)
let upper c = fn "upper" [ c ]
let lower c = fn "lower" [ c ]
let length c = fn "length" [ c ]
let trim c = fn "trim" [ c ]
let ltrim c = fn "ltrim" [ c ]
let rtrim c = fn "rtrim" [ c ]
let concat cs = fn "concat" cs
let concat_ws sep cs = fn "concat_ws" (string sep :: cs)
let substring c ~pos ~len = fn "substring" [ c; int pos; int len ]
let split c pat = fn "split" [ c; string pat ]
let regexp_replace c pat rep = fn "regexp_replace" [ c; string pat; string rep ]
let regexp_extract c pat idx = fn "regexp_extract" [ c; string pat; int idx ]
let reverse c = fn "reverse" [ c ]
let lpad c len pad = fn "lpad" [ c; int len; string pad ]
let rpad c len pad = fn "rpad" [ c; int len; string pad ]
let format_string fmt cs = fn "format_string" (string fmt :: cs)
let initcap c = fn "initcap" [ c ]
let instr c s = fn "instr" [ c; string s ]
let repeat c n = fn "repeat" [ c; int n ]
let base64 c = fn "base64" [ c ]
let unbase64 c = fn "unbase64" [ c ]
let md5 c = fn "md5" [ c ]
let sha2 c bits = fn "sha2" [ c; int bits ]

(* null handling and conditionals *)
let coalesce cs = fn "coalesce" cs
let nvl a b = fn "nvl" [ a; b ]
let nullif a b = fn "nullif" [ a; b ]
let isnull = is_null
let isnan = is_nan
let when_ = when_
let otherwise = otherwise
let if_ c a b = fn "if" [ c; a; b ]

(* dates and times *)
let current_date () = fn "current_date" []
let current_timestamp () = fn "current_timestamp" []
let now () = fn "now" []
let to_date ?fmt c = match fmt with None -> fn "to_date" [ c ] | Some f -> fn "to_date" [ c; string f ]
let to_timestamp ?fmt c = match fmt with None -> fn "to_timestamp" [ c ] | Some f -> fn "to_timestamp" [ c; string f ]
let date_format c fmt = fn "date_format" [ c; string fmt ]
let year c = fn "year" [ c ]
let month c = fn "month" [ c ]
let dayofmonth c = fn "dayofmonth" [ c ]
let dayofweek c = fn "dayofweek" [ c ]
let hour c = fn "hour" [ c ]
let minute c = fn "minute" [ c ]
let second c = fn "second" [ c ]
let date_add c n = fn "date_add" [ c; n ]
let date_sub c n = fn "date_sub" [ c; n ]
let datediff a b = fn "datediff" [ a; b ]
let add_months c n = fn "add_months" [ c; n ]
let unix_timestamp c = fn "unix_timestamp" [ c ]
let from_unixtime c = fn "from_unixtime" [ c ]
let date_trunc fmt c = fn "date_trunc" [ string fmt; c ]
let make_date y m d = fn "make_date" [ y; m; d ]

(* collections *)
let array cs = fn "array" cs
let struct_ cs = fn "struct" cs
let map cs = fn "map" cs
let map_from_arrays k v = fn "map_from_arrays" [ k; v ]
let explode c = fn "explode" [ c ]
let explode_outer c = fn "explode_outer" [ c ]
let posexplode c = fn "posexplode" [ c ]
let size c = fn "size" [ c ]
let array_contains c v = fn "array_contains" [ c; v ]
let array_distinct c = fn "array_distinct" [ c ]
let array_sort c = fn "array_sort" [ c ]
let array_max c = fn "array_max" [ c ]
let array_min c = fn "array_min" [ c ]
let array_join c sep = fn "array_join" [ c; string sep ]
let element_at c k = fn "element_at" [ c; k ]
let flatten c = fn "flatten" [ c ]
let sequence a b = fn "sequence" [ a; b ]
let map_keys c = fn "map_keys" [ c ]
let map_values c = fn "map_values" [ c ]
let transform c f = fn "transform" [ c; lambda [ "x" ] (fun vs -> f (lambda_var (List.hd vs))) ]
let filter c f = fn "filter" [ c; lambda [ "x" ] (fun vs -> f (lambda_var (List.hd vs))) ]
let exists c f = fn "exists" [ c; lambda [ "x" ] (fun vs -> f (lambda_var (List.hd vs))) ]

let aggregate c init f =
  fn "aggregate"
    [ c; init; lambda [ "acc"; "x" ] (fun vs -> f (lambda_var (List.nth vs 0)) (lambda_var (List.nth vs 1))) ]

(* json *)
let to_json c = fn "to_json" [ c ]
let from_json c schema = fn "from_json" [ c; string schema ]
let get_json_object c path = fn "get_json_object" [ c; string path ]
let json_tuple c fields = fn "json_tuple" (c :: List.map string fields)

(* window functions *)
let row_number () = fn "row_number" []
let rank () = fn "rank" []
let dense_rank () = fn "dense_rank" []
let percent_rank () = fn "percent_rank" []
let ntile n = fn "ntile" [ int n ]
let lag ?(offset = 1) ?default c = fn "lag" (c :: int offset :: Option.to_list default)
let lead ?(offset = 1) ?default c = fn "lead" (c :: int offset :: Option.to_list default)
let cume_dist () = fn "cume_dist" []
let nth_value c n = fn "nth_value" [ c; int n ]

(* misc *)
let monotonically_increasing_id () = fn "monotonically_increasing_id" []
let spark_partition_id () = fn "spark_partition_id" []
let input_file_name () = fn "input_file_name" []
let typeof c = fn "typeof" [ c ]
let assert_true c = fn "assert_true" [ c ]
let broadcast_hint = ()
