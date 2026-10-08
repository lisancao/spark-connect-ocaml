(** Spark Connect client for OCaml.

    {[
    open Spark_connect

    let () =
      Lwt_main.run
        begin
          let%lwt spark = Session.connect "sc://localhost:15002" in
          let%lwt df = Session.sql spark "SELECT id, id * 2 AS twice FROM range(5)" in
          Dataframe.show df
        end
    ]} *)

module Value = Value
module Row = Row
module Datatype = Datatype
module Column = Column
module F = Functions
module Dataframe = Dataframe
module Session = Session
module Error = Error
module Arrow = Arrow_ipc
module Arrow_writer = Arrow_writer
module Proto = Spark_connect_proto
module Client = Client
module Grpc_client = Grpc_client

exception Spark_error = Error.Spark_error
