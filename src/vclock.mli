(** Vector clock implementation *)

type t = Bigstringaf.t
(** Type of vector clocks *)

val init : unit -> t
(** Initialize a new vector clock *)

val incr : int -> t -> t
(** [incr i t] increments the counter at index i *)

val merge : t -> t -> t
(** [merge t1 t2] merges t1 and t2 to max of each index and updates both *)

val compare : t -> t -> int
(** [compare t1 t2] compares t1 and t2 returning
    0 -> equal
   -1 -> t2 dominates t1
    1 -> t1 dominates t2
    2 -> t1 concurrent t2
*)

val lca : t -> t -> t
(** [lca t1 t2] returns the lowest common ancestor of t1 and t2 *)

val sum : t -> Int64.t
(** [sum t] returns the event sum of each counter *)
