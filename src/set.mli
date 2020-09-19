(** Imperative mergeable sets.
    Operations destroy the previous version of the store *)

module type SET = sig
  type elt
  (** Type of elements in the set *)

  type t
  (** Type of mergeable set *)

  val init : int -> string -> Scylla.conn list -> t
  (** [init i s] returns a new set with replica i and ID s *)

  val is_empty : t -> bool
  (** Test if set is empty *)

  val mem : elt -> t -> bool
  (** [mem x] tests if [x] exists in the set *)

  val add : elt -> t -> unit
  (** [add x s] adds [x] to the set *)

  val remove : elt -> t -> unit
  (** [remove x] removes [x] from the set *)

  val merge : Vclock.t -> int -> t -> Vclock.t
  (** [merge v i] performs a 3-way merge with version [v] at replica [i] updating the set *)

  val commit : t -> Vclock.t
  (** [commit ()] exposes the data to the world *)

  val update : Vclock.t -> t -> unit
  (** [set v] sets the data version to v *)

  val packet : t -> (Vclock.t * int * string)
  (** [packet t] returns [(vector, replica, variable_id)] *)
end

module Make (Ord : S.ORDERED) : SET
  with type elt = Ord.t
