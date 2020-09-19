(** Imperative mergeable sets.
    Operations destroy the previous version of the store *)

module type SET = sig
  type elt
  (** Type of elements in the set *)

  type t
  (** Type of mergeable set *)

  val init : int -> string -> Scylla.conn list -> t
  (** [init i s] returns a new set with replica id i and ID s *)

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
  with type elt = Ord.t =
struct

  (* in-memory representation of the set *)
  module Rbtree = Rbtree.Make(Ord)

  (* persistent store for the tree *)
  module Store = Store.Make(struct type t = Rbtree.t let t = Rbtree.t end)

  (* type of set *)
  type t = {
    mutable vec : Vclock.t
  ; mutable value : Rbtree.t
  ; store : Store.t
  ; replica : int
  ; id : string}

  let init i s l =
    (* initialize with latest value *)
    let store = Store.init Rbtree.empty i s l in
    let (vec, value) = Store.latest store in
    { vec ; value ; store ; replica = i ; id = s}

  type elt = Ord.t

  let is_empty t = Rbtree.is_empty t.value

  let mem x t = Rbtree.mem x t.value

  let add x t = t.value <- Rbtree.add x t.value

  let remove x t = t.value <- Rbtree.remove x t.value

  let merge_vals v0 v1 v2 =
    let s1 = Rbtree.inter (Rbtree.inter v1 v2) v0 in
    let s2 = Rbtree.diff v1 v0 in
    let s3 = Rbtree.diff v2 v0 in
    Rbtree.union (Rbtree.union s1 s2) s3

  let merge target_vec target_replica t =
    (* vector clocks *)
    let v0_clock = Vclock.lca t.vec target_vec in
    let v1_clock = t.vec in
    let v2_clock = target_vec in

    (* values *)
    let v0 = Store.find v0_clock t.store |> Option.get in
    let v1 = Store.find v1_clock t.store |> Option.get in
    let v2 = Store.find v2_clock t.store |> Option.get in

    (* perform merge *)
    let n_clock = Vclock.merge v1_clock v2_clock in
    let n_value  = merge_vals v0 v1 v2 in

    (* commit the value *)
    let _ = Store.add n_clock n_value ~actor2:target_replica t.store in
    t.vec <- n_clock;
    t.value <- n_value;
    n_clock

  let commit t =
    let n_clock = Vclock.incr t.replica t.vec in
    let _ = Store.add n_clock t.value t.store in
    t.vec <- n_clock;
    t.vec

  let update v t =
    t.vec <- v;
    t.value <- Store.find v t.store |> Option.get

  let packet t =
    (t.vec, t.replica, t.id)
end
