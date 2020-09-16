(** Versioned stores *)
open Scylla
open Scylla.Protocol

(** Signature for versioned store *)
module type STORE = sig
  type elt
  (** Type of elements in store *)
   
  val init : elt -> unit
  (** Initialize the store with a value v *)

  (*
  val latest : 'a Irmin.Type.ty -> (Vclock.t * Int64.t * 'a)
  (** [latest ('a ty)] returns a value as [(vclock, version, 'a)] *)

  *)
  val add : Vclock.t -> elt -> ?actor2:int -> unit -> string
  (** [add vec ver val] adds [val] to the store *)

  val find : Vclock.t -> string -> elt option

  (*
  val mem : Vclock.t -> string -> bool
  (** [mem vec val] checks if [val] with [vec] version exists in store *)
   *)
end

(** Implementaion of versioned store *)
module Make (Replica : S.REPLICA) (Type : S.TYPE): STORE with type elt = Type.t = struct
  type elt = Type.t
              
  let conn = List.hd Replica.servers

  let create_meta_query = Printf.sprintf
    "create table if not exists mrdt.%s_meta(
    vec blob,
    key blob,
    actor1 int,
    actor2 int,
    ver bigint,
    primary key (vec))" Replica.variable_id

  let create_value_query = Printf.sprintf
    "create table if not exists mrdt.%s_value(
    key blob,
    value blob,
    primary key (key))" Replica.variable_id

  let insert_meta_query = Printf.sprintf
    "insert into mrdt.%s_meta(
    vec,
    key,
    actor1,
    actor2,
    ver)
    VALUES (?,?,?,?,?)" Replica.variable_id

  let insert_value_query = Printf.sprintf
    "insert into mrdt.%s_value(
    key,
    value)
    VALUES (?,?)" Replica.variable_id

  let select_meta_query = Printf.sprintf
    "select key from mrdt.%s_meta where vec = ?" Replica.variable_id

  let select_value_query = Printf.sprintf
    "select value from mrdt.%s_value where key = ?" Replica.variable_id

  let to_bin_string = Irmin.Type.to_bin_string

  let of_bin_string = Irmin.Type.of_bin_string

  let get_value v =
    let v = (function Blob b -> b | _ -> failwith "error") v in
    Bigstringaf.to_string v

  let init_val v =
    let rep = to_bin_string Type.t v in
    let bin = Bytes.of_string rep in
    let hash = Digest.bytes bin in
    let key = Blob (Bigstringaf.of_string hash ~off:0 ~len:(String.length hash)) in
    let actor1 = Int (Int32.of_int Replica.replica_id) in
    let actor2 = Int (-1l) in
    let ver = Bigint (0L) in
    let vec = Blob (Vclock.init ()) in
    let value = Bigstringaf.create (Bytes.length bin) in
    Bigstringaf.blit_from_bytes bin ~src_off:0 value ~dst_off:0 ~len:(Bytes.length bin);
    let value = Blob value in
    (key, vec, actor1, actor2, ver, value)

  let make_key k =
    Blob (Bigstringaf.of_string k ~off:0 ~len:(String.length k))

  let make_key_value v =
    let rep = to_bin_string Type.t v in
    let bin = Bytes.of_string rep in
    let hash = Digest.bytes bin in
    let key = Blob (Bigstringaf.of_string hash ~off:0 ~len:(String.length hash)) in
    let value = Bigstringaf.create (Bytes.length bin) in
    Bigstringaf.blit_from_bytes bin ~src_off:0 value ~dst_off:0 ~len:(Bytes.length bin);
    let value = Blob value in
    (key, hash, value)

  let init v =
    let open Result in
    (* create the tables *)
    let _ = query conn ~query:create_meta_query () |> get_ok in
    let _ = query conn ~query:create_value_query () |> get_ok in
    let (key, vec, actor1, actor2, ver, value) = init_val v in
    let _ = query conn ~query:insert_meta_query
      ~values:[|vec ; key ; actor1 ; actor2 ; ver |] () |> get_ok in
    let _ = query conn ~query:insert_value_query
      ~values:[|key ; value |] () |> get_ok in
    ()

  let add vec value ?actor2:(actor2 : int option) () =
    let (key, hash, value) = make_key_value value in
    let actor1 = Int (Int32.of_int Replica.replica_id) in
    let actor2 = Int ((function Some i -> Int32.of_int i | None -> -1l) actor2) in
    let ver = Bigint (Vclock.sum vec) in
    let vec = Blob vec in
    let _ = query conn ~query:insert_value_query
        ~values:[|key ; value|] () |> Result.get_ok in
    let _ = query conn ~query:insert_meta_query
        ~values:[|vec ; key ; actor1 ; actor2 ; ver |] () |> Result.get_ok in
    hash

  let find vec key =
    let key = make_key key in
    let vec = Blob vec in
    let res = (query conn ~query:select_meta_query
      ~values:[| vec |] () |> Result.get_ok).values in
    if (Array.length res = 0) then
      None
    else begin
      let value = get_value (query conn ~query:select_value_query
        ~values:[|key|] () |> Result.get_ok).values.(0).(0) in
      let value = of_bin_string Type.t value |> Result.get_ok in
      Some value
    end
end
