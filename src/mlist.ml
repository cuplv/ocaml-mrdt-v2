module type LIST = sig
  type elt
  (** Type of elements in the list *)

  type t
  (** Type of mergeable list *)

  (*
  val init : int -> string -> Scylla.conn list -> t
  (** [init i s] returns a new list with replica i and ID s *)

  val cons : elt -> t -> unit
  (** Adds an element to the head of the list *)

  val peek : t -> elt
  (** Peek head element of lsit *)

  val merge : Vclock.t -> int -> t -> Vclock.t
  (** [merge v i] performs a 3-way merge with version [v] at replica [i] updating the set *)

  val commit : t -> Vclock.t
  (** [commit t] exposes the data to the world *)

  val update : Vclock.t -> t -> unit
  (** [set v] sets the data version to v *)

  val packet : t -> (Vclock.t * int * string)
  (** [packet t] returns [(vector, replica, variable_id)] *)
  *)
end

module Make (Typ : S.TYPE) : LIST
  with type elt = Typ.t =
struct

  (* pair each element with a number *)
  type s = int * Typ.t
  [@@deriving irmin]

  let compare_pair x y = compare (fst x) (fst y)

  module TypSet = Mset.Make (struct
    type t = s
    let t = s_t
    let compare = compare_pair
  end)

  type elt = Typ.t

  type t = TypSet.t
end
