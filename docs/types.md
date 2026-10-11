# Results and types

Results arrive as Arrow batches and decode into OCaml values. Snippets assume
`open Spark_connect`.

## Rows

`Dataframe.collect` returns `Row.t list`. A row keeps column names and values in order:

| Function | Returns |
|---|---|
| `Row.field r "name"` | the value of a column (raises if missing) |
| `Row.field_opt r "name"` | `Value.t option` |
| `Row.get r i` | the value at a position |
| `Row.names r`, `Row.values r` | arrays of names and values |
| `Row.to_list r`, `Row.to_alist r` | as a list, or as `(name, value)` pairs |
| `Row.to_string r` | `[1, ann, 15.5]`-style rendering |

For large results, `Dataframe.collect_columns` returns the column names and one
`Value.t array` per column, which avoids building a row per record.

## Values

```ocaml
type Value.t =
  | Null
  | Bool of bool
  | Int of int            (* 8-, 16- and 32-bit integers *)
  | Int64 of int64        (* bigint *)
  | Float of float        (* float and double *)
  | String of string
  | Binary of string
  | Decimal of string     (* rendered in decimal notation, e.g. "-12.340" *)
  | Date of int           (* days since 1970-01-01 *)
  | Time of int64 * time_unit
  | Timestamp of int64 * time_unit * string option   (* value, unit, time zone *)
  | Duration of int64 * time_unit                     (* interval day to second *)
  | Interval_year_month of int                       (* months *)
  | Interval_day_time of int * int
  | Interval_month_day_nano of int * int * int64
  | List of t array
  | Struct of (string * t) array
  | Map of (t * t) array
```

Accessors raise on a type mismatch; the `_opt` variants map `Null` to `None`:

| Accessor | For |
|---|---|
| `Value.to_int`, `to_int_opt` | `Int` and `Int64` |
| `Value.to_int64`, `to_int64_opt` | `Int` and `Int64` |
| `Value.to_float`, `to_float_opt` | `Float` (and integers) |
| `Value.to_bool`, `to_bool_opt` | `Bool` |
| `Value.to_str`, `to_str_opt` | `String` |
| `Value.to_string` | any value, rendered for display |

## Spark type mapping

| Spark | `Value.t` |
|---|---|
| tinyint, smallint, int | `Int` |
| bigint | `Int64` |
| float, double | `Float` |
| decimal(p, s) | `Decimal` (a string, so no precision is lost) |
| string, binary | `String`, `Binary` |
| date | `Date` |
| timestamp | `Timestamp (_, Microsecond, Some zone)`, in the session time zone |
| timestamp_ntz | `Timestamp (_, Microsecond, None)` |
| interval year to month, day to second | `Interval_year_month`, `Duration` |
| array, struct, map | `List`, `Struct`, `Map` |

Timestamps render in the session time zone (`spark.sql.session.timeZone`), so set it
explicitly when you compare rendered values across machines.

## Sending values

`Session.create_dataframe` and SQL parameters take `Value.t` too. The server infers
Spark types from the Arrow schema the client sends: `Int` and `Int64` become `bigint`,
`Float` becomes `double`, `String` becomes `string`, and so on. Pass a DDL schema to
`Session.create_dataframe_ddl` for narrower types.

## Schemas

`Dataframe.schema` returns a `Datatype.t` (the protocol's data type);
`Datatype.simple_string` renders it as `struct<id:bigint,name:string>`, and
`Dataframe.dtypes` gives `(name, type)` pairs. `Datatype` also has constructors
(`Datatype.long ()`, `Datatype.struct_ [ ... ]`, ...) for `Dataframe.to_schema`.
