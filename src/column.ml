(* Column expressions: thin builders over the [Expression] protobuf. *)

module P = Spark_connect_proto
module E = P.Expressions
module T = P.Types

type t = E.expression

let of_expr_type et : t = E.make_expression ~expr_type:et ()
let expr_type (c : t) = c.expr_type

(* ---- literals ---- *)

let rec infer_type (v : Value.t) : T.data_type =
  match v with
  | Null -> Datatype.null ()
  | Bool _ -> Datatype.boolean ()
  | Int i -> if i >= -2147483648 && i <= 2147483647 then Datatype.integer () else Datatype.long ()
  | Int64 _ -> Datatype.long ()
  | Float _ -> Datatype.double ()
  | String _ -> Datatype.string ()
  | Binary _ -> Datatype.binary ()
  | Decimal s ->
      let p, sc = decimal_shape s in
      Datatype.decimal ~precision:p ~scale:sc
  | Date _ -> Datatype.date ()
  | Time _ -> T.Time (T.make_data_type_time ~precision:6l ())
  | Timestamp (_, _, Some _) -> Datatype.timestamp ()
  | Timestamp (_, _, None) -> Datatype.timestamp_ntz ()
  | Duration _ -> T.Day_time_interval (T.make_data_type_day_time_interval ~start_field:0l ~end_field:3l ())
  | Interval_year_month _ ->
      T.Year_month_interval (T.make_data_type_year_month_interval ~start_field:0l ~end_field:1l ())
  | Interval_day_time _ | Interval_month_day_nano _ -> T.Calendar_interval (T.make_data_type_calendar_interval ())
  | List vs -> Datatype.array (if Array.length vs > 0 then infer_type vs.(0) else Datatype.null ())
  | Struct fs -> Datatype.struct_ (Array.to_list (Array.map (fun (k, v) -> (k, infer_type v, true)) fs))
  | Map kvs ->
      if Array.length kvs > 0 then Datatype.map (infer_type (fst kvs.(0))) (infer_type (snd kvs.(0)))
      else Datatype.map (Datatype.null ()) (Datatype.null ())

(* precision/scale of a decimal string like "-123.450" *)
and decimal_shape s =
  let s = if String.length s > 0 && (s.[0] = '-' || s.[0] = '+') then String.sub s 1 (String.length s - 1) else s in
  match String.index_opt s '.' with
  | None -> (max 1 (String.length s), 0)
  | Some i ->
      let scale = String.length s - i - 1 in
      let int_digits = i in
      (max 1 (int_digits + scale), scale)

let to_micros v unit =
  let open Value in
  match unit with
  | Microsecond -> v
  | Millisecond -> Int64.mul v 1000L
  | Second -> Int64.mul v 1_000_000L
  | Nanosecond -> Int64.div v 1000L

let rec literal_of_value (v : Value.t) : E.expression_literal =
  let lit ?data_type lt = E.make_expression_literal ~literal_type:lt ?data_type () in
  match v with
  | Null -> lit (E.Null (Datatype.null ()))
  | Bool b -> lit (E.Boolean b)
  | Int i ->
      if i >= -2147483648 && i <= 2147483647 then lit (E.Integer (Int32.of_int i)) else lit (E.Long (Int64.of_int i))
  | Int64 i -> lit (E.Long i)
  | Float f -> lit (E.Double f)
  | String s -> lit (E.String s)
  | Binary b -> lit (E.Binary (Bytes.of_string b))
  | Decimal s ->
      let p, sc = decimal_shape s in
      lit
        (E.Decimal (E.make_expression_literal_decimal ~value:s ~precision:(Int32.of_int p) ~scale:(Int32.of_int sc) ()))
  | Date d -> lit (E.Date (Int32.of_int d))
  | Time (v, unit) ->
      lit (E.Time (E.make_expression_literal_time ~nano:(Int64.mul (to_micros v unit) 1000L) ~precision:6l ()))
  | Timestamp (v, unit, Some _) -> lit (E.Timestamp (to_micros v unit))
  | Timestamp (v, unit, None) -> lit (E.Timestamp_ntz (to_micros v unit))
  | Duration (v, unit) -> lit (E.Day_time_interval (to_micros v unit))
  | Interval_year_month m -> lit (E.Year_month_interval (Int32.of_int m))
  | Interval_day_time (d, ms) ->
      lit
        (E.Calendar_interval
           (E.make_expression_literal_calendar_interval ~months:0l ~days:(Int32.of_int d)
              ~microseconds:(Int64.mul (Int64.of_int ms) 1000L)
              ()))
  | Interval_month_day_nano (m, d, ns) ->
      lit
        (E.Calendar_interval
           (E.make_expression_literal_calendar_interval ~months:(Int32.of_int m) ~days:(Int32.of_int d)
              ~microseconds:(Int64.div ns 1000L) ()))
  | List vs ->
      let element_type = if Array.length vs > 0 then infer_type vs.(0) else Datatype.null () in
      lit
        (E.Array
           (E.make_expression_literal_array ~element_type ~elements:(Array.to_list (Array.map literal_of_value vs)) ()))
  | Struct fs ->
      let struct_type = infer_type v in
      lit
        (E.Struct
           (E.make_expression_literal_struct ~struct_type
              ~elements:(Array.to_list (Array.map (fun (_, v) -> literal_of_value v) fs))
              ()))
  | Map kvs ->
      let key_type, value_type =
        if Array.length kvs > 0 then (infer_type (fst kvs.(0)), infer_type (snd kvs.(0)))
        else (Datatype.null (), Datatype.null ())
      in
      lit
        (E.Map
           (E.make_expression_literal_map ~key_type ~value_type
              ~keys:(Array.to_list (Array.map (fun (k, _) -> literal_of_value k) kvs))
              ~values:(Array.to_list (Array.map (fun (_, v) -> literal_of_value v) kvs))
              ()))

let lit v : t = of_expr_type (E.Literal (literal_of_value v))
let int i = lit (Value.Int i)
let int64 i = lit (Value.Int64 i)
let float f = lit (Value.Float f)
let string s = lit (Value.String s)
let bool b = lit (Value.Bool b)
let null = lit Value.Null
let date d = lit (Value.Date d)

let typed_null (dt : T.data_type) : t =
  of_expr_type (E.Literal (E.make_expression_literal ~literal_type:(E.Null dt) ()))

(* ---- references ---- *)

let col name : t =
  of_expr_type (E.Unresolved_attribute (E.make_expression_unresolved_attribute ~unparsed_identifier:name ()))

let col_of_plan ~plan_id name : t =
  of_expr_type (E.Unresolved_attribute (E.make_expression_unresolved_attribute ~unparsed_identifier:name ~plan_id ()))

let star : t = of_expr_type (E.Unresolved_star (E.make_expression_unresolved_star ()))
let star_of target : t = of_expr_type (E.Unresolved_star (E.make_expression_unresolved_star ~unparsed_target:target ()))
let expr s : t = of_expr_type (E.Expression_string (E.make_expression_expression_string ~expression:s ()))
let regex pattern : t = of_expr_type (E.Unresolved_regex (E.make_expression_unresolved_regex ~col_name:pattern ()))

(* ---- functions and operators ---- *)

let fn ?(distinct = false) name (args : t list) : t =
  of_expr_type
    (E.Unresolved_function
       (E.make_expression_unresolved_function ~function_name:name ~arguments:args ~is_distinct:distinct ()))

let internal_fn name args : t =
  of_expr_type
    (E.Unresolved_function
       (E.make_expression_unresolved_function ~function_name:name ~arguments:args ~is_internal:true ()))

let alias (c : t) name : t = of_expr_type (E.Alias (E.make_expression_alias ~expr:c ~name:[ name ] ()))
let alias_multi (c : t) names : t = of_expr_type (E.Alias (E.make_expression_alias ~expr:c ~name:names ()))
let as_ = alias
let name = alias
let cast (c : t) type_ : t = of_expr_type (E.Cast (E.make_expression_cast ~expr:c ~cast_to_type:(E.Type_str type_) ()))

let cast_to (c : t) (dt : T.data_type) : t =
  of_expr_type (E.Cast (E.make_expression_cast ~expr:c ~cast_to_type:(E.Type dt) ()))

let try_cast (c : t) type_ : t =
  of_expr_type (E.Cast (E.make_expression_cast ~expr:c ~cast_to_type:(E.Type_str type_) ~eval_mode:E.Eval_mode_try ()))

let sort_order ?(nulls : [ `First | `Last ] option) (c : t) dir : E.expression_sort_order =
  E.make_expression_sort_order ~child:c ~direction:dir
    ~null_ordering:
      (match nulls with
      | None -> E.Sort_nulls_unspecified
      | Some `First -> E.Sort_nulls_first
      | Some `Last -> E.Sort_nulls_last)
    ()

let asc c : t = of_expr_type (E.Sort_order (sort_order c E.Sort_direction_ascending))
let desc c : t = of_expr_type (E.Sort_order (sort_order c E.Sort_direction_descending))
let asc_nulls_first c : t = of_expr_type (E.Sort_order (sort_order ~nulls:`First c E.Sort_direction_ascending))
let asc_nulls_last c : t = of_expr_type (E.Sort_order (sort_order ~nulls:`Last c E.Sort_direction_ascending))
let desc_nulls_first c : t = of_expr_type (E.Sort_order (sort_order ~nulls:`First c E.Sort_direction_descending))
let desc_nulls_last c : t = of_expr_type (E.Sort_order (sort_order ~nulls:`Last c E.Sort_direction_descending))

(** Any expression as a sort order (ascending unless it already is one). *)
let to_sort_order (c : t) : E.expression_sort_order =
  match c.expr_type with Some (E.Sort_order so) -> so | _ -> sort_order c E.Sort_direction_ascending

let add a b = fn "+" [ a; b ]
let sub a b = fn "-" [ a; b ]
let mul a b = fn "*" [ a; b ]
let div a b = fn "/" [ a; b ]
let rem a b = fn "%" [ a; b ]
let neg a = fn "negative" [ a ]
let eq a b = fn "==" [ a; b ]
let eq_null_safe a b = fn "<=>" [ a; b ]
let neq a b = fn "not" [ fn "==" [ a; b ] ]
let lt a b = fn "<" [ a; b ]
let le a b = fn "<=" [ a; b ]
let gt a b = fn ">" [ a; b ]
let ge a b = fn ">=" [ a; b ]
let and_ a b = fn "and" [ a; b ]
let or_ a b = fn "or" [ a; b ]
let not_ a = fn "not" [ a ]
let is_null a = fn "isnull" [ a ]
let is_not_null a = fn "isnotnull" [ a ]
let is_nan a = fn "isnan" [ a ]
let like a pat = fn "like" [ a; string pat ]
let ilike a pat = fn "ilike" [ a; string pat ]
let rlike a pat = fn "rlike" [ a; string pat ]
let starts_with a s = fn "startswith" [ a; s ]
let ends_with a s = fn "endswith" [ a; s ]
let contains a s = fn "contains" [ a; s ]
let substr a ~pos ~len = fn "substr" [ a; pos; len ]
let isin a vs = fn "in" (a :: vs)
let between a ~low ~high = and_ (ge a low) (le a high)
let bitwise_and a b = fn "&" [ a; b ]
let bitwise_or a b = fn "|" [ a; b ]
let bitwise_xor a b = fn "^" [ a; b ]

let get_item (c : t) (key : t) : t =
  of_expr_type (E.Unresolved_extract_value (E.make_expression_unresolved_extract_value ~child:c ~extraction:key ()))

let get_field c name = get_item c (string name)
let item c i = get_item c (int i)

let with_field (c : t) field_name (v : t) : t =
  of_expr_type
    (E.Update_fields (E.make_expression_update_fields ~struct_expression:c ~field_name ~value_expression:v ()))

let drop_fields (c : t) names : t =
  List.fold_left
    (fun acc n ->
      of_expr_type (E.Update_fields (E.make_expression_update_fields ~struct_expression:acc ~field_name:n ())))
    c names

(* CASE WHEN: [when c v] starts a chain; [when_ e c v] extends; [otherwise e v] ends it. *)
let when_ cond v = fn "when" [ cond; v ]

let add_when (c : t) cond v : t =
  match c.expr_type with
  | Some (E.Unresolved_function f) when f.function_name = "when" -> fn "when" (f.arguments @ [ cond; v ])
  | _ -> invalid_arg "Column.add_when: not a when() expression"

let otherwise (c : t) v : t =
  match c.expr_type with
  | Some (E.Unresolved_function f) when f.function_name = "when" -> fn "when" (f.arguments @ [ v ])
  | _ -> invalid_arg "Column.otherwise: not a when() expression"

(* ---- windows ---- *)

module Window = struct
  type frame_boundary = Unbounded | Current_row | Value of t

  type spec = {
    partition_by : t list;
    order_by : E.expression_sort_order list;
    frame : (E.expression_window_window_frame_frame_type * frame_boundary * frame_boundary) option;
  }

  let empty = { partition_by = []; order_by = []; frame = None }
  let partition_by cols = { empty with partition_by = cols }
  let order_by cols = { empty with order_by = List.map to_sort_order cols }
  let with_partition_by s cols = { s with partition_by = cols }
  let with_order_by s cols = { s with order_by = List.map to_sort_order cols }
  let rows_between s lo hi = { s with frame = Some (E.Frame_type_row, lo, hi) }
  let range_between s lo hi = { s with frame = Some (E.Frame_type_range, lo, hi) }
  let unbounded_preceding = Unbounded
  let unbounded_following = Unbounded
  let current_row = Current_row
  let boundary = function Unbounded -> E.Unbounded true | Current_row -> E.Current_row true | Value v -> E.Value v

  let frame_spec = function
    | None -> None
    | Some (ft, lo, hi) ->
        Some (E.make_expression_window_window_frame ~frame_type:ft ~lower:(boundary lo) ~upper:(boundary hi) ())
end

let over (c : t) (w : Window.spec) : t =
  of_expr_type
    (E.Window
       (E.make_expression_window ~window_function:c ~partition_spec:w.partition_by ~order_spec:w.order_by
          ?frame_spec:(Window.frame_spec w.frame) ()))

(* ---- lambdas (for higher-order functions) ---- *)

let lambda_var name : t =
  of_expr_type
    (E.Unresolved_named_lambda_variable (E.make_expression_unresolved_named_lambda_variable ~name_parts:[ name ] ()))

let lambda (params : string list) (body : string list -> t) : t =
  let args = List.map (fun n -> E.make_expression_unresolved_named_lambda_variable ~name_parts:[ n ] ()) params in
  of_expr_type (E.Lambda_function (E.make_expression_lambda_function ~function_:(body params) ~arguments:args ()))

module O = struct
  let ( + ) = add
  let ( - ) = sub
  let ( * ) = mul
  let ( / ) = div
  let ( % ) = rem
  let ( = ) = eq
  let ( <> ) = neq
  let ( < ) = lt
  let ( <= ) = le
  let ( > ) = gt
  let ( >= ) = ge
  let ( && ) = and_
  let ( || ) = or_
  let not = not_

  (** [c @= "name"] is [alias c "name"] *)
  let ( @= ) = alias
end
