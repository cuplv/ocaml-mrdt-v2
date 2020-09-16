(** Versioned stores *)
open Scylla
open Scylla.Protocol

(** Signature for versioned store *)
module type STORE = sig
  type elt
  (** Type of elements in store *)
   
  val init : elt -> unit
  (** Initialize the store with a value v *)

  val mem : Vclock.t -> string -> bool
  (** [mem vec key] is true iff [vec key] present in store *)

  val add : Vclock.t -> elt -> ?actor2:int -> unit -> string
  (** [add vec ver val] adds [val] to the store *)

  val find : Vclock.t -> string -> elt option
  (** [find vec key] returns [Some val] if value exists or [None] *)

  val latest: unit -> (Vclock.t * string)
  (** [latest ()] returns the latest version for the replica *)
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

  let select_latest_query1 = Printf.sprintf
    "select vec, key, ver from mrdt.%s_meta where actor1 = %d and ver > -1 allow filtering" Replica.variable_id Replica.replica_id

  let select_latest_query2 = Printf.sprintf
    "select vec, key, ver from mrdt.%s_meta where actor2 = %d and ver > -1 allow filtering" Replica.variable_id Replica.replica_id

  let select_root_query = Printf.sprintf
    "select vec, key, ver from mrdt.%s_meta where actor1 = -1 and actor2 = -1 allow filtering" Replica.variable_id

  let to_bin_string = Irmin.Type.to_bin_string

  let of_bin_string = Irmin.Type.of_bin_string

  let get_blob = function Blob b -> b | _ -> failwith "error"

  let get_value v =
    let v = (function Blob b -> b | _ -> failwith "error") v in
    Bigstringaf.to_string v

  let get_int64 = function Bigint i -> i | _ -> failwith "error"

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
    let (key, vec, _, actor2, ver, value) = init_val v in
    let actor1 = Int (-1l) in
    let _ = query conn ~query:insert_meta_query
      ~values:[|vec ; key ; actor1 ; actor2 ; ver |] () |> get_ok in
    let _ = query conn ~query:insert_value_query
      ~values:[|key ; value |] () |> get_ok in
    ()

  let mem vec _key =
    let vec = Blob vec in
    let res = (query conn ~query:select_meta_query
      ~values:[| vec |] () |> Result.get_ok).values in
    Array.length res > 0

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

  let latest _ =
    let values1 = (query conn ~query:select_latest_query1 () |> Result.get_ok).values in
    let values2 = (query conn ~query:select_latest_query2 () |> Result.get_ok).values in
    let values = Array.concat [values1 ; values2] in
    let values = if (Array.length values) = 0 then
      (query conn ~query:select_root_query () |> Result.get_ok).values
      else values
    in
    let values = Array.map (fun i -> (get_blob i.(0) , get_value i.(1) , get_int64 i.(2))) values in
    let max_ver = ref (-1L) in
    let max_ind = ref (-1) in
    for i = 0 to (Array.length values) - 1 do
      let (_, _, ver) = values.(i) in
      if (ver > !max_ver) then begin
        max_ver := ver;
        max_ind := i
      end else
        ()
    done;
    let (vec, key, _) = values.(!max_ind) in
    (vec, key)
end
