(** Versioned stores *)

(** Signature for versioned store *)
module type STORE = sig
  type elt
  (** Type of elements in store *)

  val init : elt -> unit
  (** Initialize the store with a value v *)

  val mem : Vclock.t -> string -> bool
  (** [mem vec key] is true iff [vec key] present in store *)

  val add : Vclock.t -> elt -> ?actor2:int -> unit -> string
  (** [add vec ver val] adds [val] to the store *)

  val find : Vclock.t -> string -> elt option
  (** [find vec key] returns [Some val] if value exists or [None] *)

  val latest: unit -> (Vclock.t * string)
  (** [latest ()] returns the latest version for the replica *)
end

(** Implementaion of versioned store *)
module Make (Replica : S.REPLICA) (Type : S.TYPE) : STORE
  with type elt = Type.t
