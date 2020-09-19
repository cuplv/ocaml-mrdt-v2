(** Implementation of sync for mergeable types *)

val sync :
(module S.RPC) -> (module S.MERGE with type t = 'a) -> int -> 'a -> unit Lwt.t
