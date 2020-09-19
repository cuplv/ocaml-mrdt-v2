open Lwt.Syntax
      
let sync (type t) (module Rpc : S.RPC) (module Merge : S.MERGE with type t = t) dst_replica t =
  let (vec, replica, id) = Merge.packet t in
  let* n_vec = Rpc.peer_merge vec replica id dst_replica in
  Merge.update n_vec t;
  Lwt.return ()
