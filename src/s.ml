(** Common modules and module types *)

(** Signature of struct used to initiate replicas of a datatype *)
module type REPLICA = sig
  val replica_id : int
  (** Replica id *)

  val variable_id : string
  (** Identifier for a variable *)

  val servers : Scylla.conn list
  (** scylladb server connections servicing store requests *)
end

module type TYPE = sig
  type t
  (** Type of data *)

  val t : t Irmin.Type.ty
  (** Type representation of data *)
end
