(** Imperative mergeable sets.
    Operations destroy the previous version of the store *)

module type OrdElt = sig
  type t
  (** Type of element to be in set *)

  val t : 'a Irmin.Type.ty
  (** Type representation of elements in the set *)
end

module type SET = sig
  type elt
  (** Type of elements in the set *)

  val empty : unit -> unit
  (** Empty the current set *)

  val is_empty : unit -> bool
  (** Test if set is empty *)

  val mem : elt -> bool
  (** [mem x] tests if [x] exists in the set *)

  val add : elt -> unit
  (** [add x s] adds [x] to the set *)

  val remove : elt -> unit
  (** [remove x] removes [x] from the set *)

  val sync : Vclock.t -> unit
  (** [sync i] performs a 3-way merge with version [v] updating the set *)
end

module Make (Ord : OrdElt) (Replica : S.REPLICA) : SET = struct
  type elt = Ord.t

  (* irmin type representation of values *)
  let _elt = Ord.t

  (* internal rep *)
  (* type internal = {vector : Vclock.t ref ; version : Int64.t ref; value : EltSet.t ref} *)

  let empty _ = ()

  let is_empty () = false

  let mem _ = false

  let add _ = ()

  let remove _ = ()

  let sync _ = ()
end
