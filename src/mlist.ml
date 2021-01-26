module type LIST = sig
  type elt
  (** Type of elements in the list *)

  type t
  (** Type of mergeable list *)

  val init : int -> string -> Scylla.conn list -> t
  (** [init i s] returns a new list with replica i and ID s *)

  val cons : elt -> t -> unit
  (** Adds an element to the head of the list *)

  val peek : t -> elt
  (** Peek head element of list *)

  val iter : (elt -> unit) -> t -> unit
  (** [iter t f] will apply f to each element *)

  val merge : Vclock.t -> int -> t -> Vclock.t
  (** [merge v i] performs a 3-way merge with version [v] at replica [i] updating the set *)

  val commit : t -> Vclock.t
  (** [commit t] exposes the data to the world *)

  val update : Vclock.t -> t -> unit
  (** [set v] sets the data version to v *)

  val packet : t -> (Vclock.t * int * string)
  (** [packet t] returns [(vector, replica, variable_id)] *)
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

  type t = { mutable index : int ; set : TypSet.t }

  let init i s c =
    let set = TypSet.init i s c in
    match (TypSet.is_empty set) with
      | true -> { index = max_int ; set }
      | false -> { index = fst (TypSet.min_elt set) ; set }

  let cons elt t = 
    TypSet.add (t.index - 1, elt) t.set;
    t.index <- t.index - 1

  let peek t = snd @@ TypSet.min_elt t.set

  let iter f t = TypSet.iter (fun s -> f @@ snd s) t.set

  let merge v i t = TypSet.merge v i t.set

  let commit t = TypSet.commit t.set

  let update v t = TypSet.update v t.set

  let packet t = TypSet.packet t.set
end
