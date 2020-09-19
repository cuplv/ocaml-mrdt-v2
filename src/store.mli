(** Versioned stores *)

(** Signature for versioned store *)
module type STORE = sig
  type elt
  (** Type of elements in store *)

  type t
  (** Type of store *)

  val init : elt -> int -> string -> Scylla.conn list -> t
  (** Initialize the store with a value v *)

  val mem : Vclock.t -> elt -> t -> bool
  (** [mem vec key] is true iff [vec key] present in store *)

  val add : Vclock.t -> elt -> ?actor2:int -> t -> string
  (** [add vec ver val] adds [val] to the store *)

  val find : Vclock.t -> t -> elt option
  (** [find vec key] returns [Some val] if value exists or [None] *)

  val latest: t -> (Vclock.t * elt)
  (** [latest ()] returns the latest version for the replica *)
end

(** Implementaion of versioned store *)
module Make (Type : S.TYPE) : STORE
  with type elt = Type.t
