(** Common modules and module types *)

(** Signature of a type and its representation *)
module type TYPE = sig
  type t
  (** Type of data *)

  val t : t Irmin.Type.ty
  (** Type representation of data *)
end

(** Signature of a type with a comparison function *)
module type ORDERED = sig
  include TYPE
  (** Type of data and its representation *)
     
  val compare : t -> t -> int
  (** Comparison function for elements *)
end

(** Signature of a type that allows commit and merge operations *)
module type MERGE = sig
  type t
  (** Type of mergeable data structure *)

  val merge : Vclock.t -> int -> t -> Vclock.t
  (** [merge v i t] performs a 3 way merge with version [v]
      at replica [i] and returns updated vector *)

  val commit : t -> Vclock.t
  (** [commit t] persists the data and returns the latest version vector *)

  val update : Vclock.t -> t -> unit
  (** [update v t] updates the latest version and data for [t] *)

  val packet : t -> (Vclock.t * int * string)
  (** [packet t] returns [(vector, replica, variable_id)] *)
end

(** RPC module for peer-to-peer communication *)
module type RPC = sig
  val peer_merge : Vclock.t -> int -> string -> int -> Vclock.t Lwt.t
  (** [peer_merge vec src_replica variable_id dst_replica]
      requests [dst_replica] to commit its values and then
      merge the incoming request and return the updated vclock *)
end
