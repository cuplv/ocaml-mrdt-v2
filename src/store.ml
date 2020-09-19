(** Versioned stores *)
open Scylla
open Scylla.Protocol

(** Signature for versioned store *)
module type STORE = sig
  type elt
  (** Type of elements in store *)

  type t
  (** Type of store *)
   
  val init : elt -> int -> string -> Scylla.conn list -> t
  (** Initialize the store with a value v *)

  val mem : Vclock.t -> elt -> t -> bool
  (** [mem vec key] is true iff [vec key] present in store *)

  val add : Vclock.t -> elt -> ?actor2:int -> t -> string
  (** [add vec ver val] adds [val] to the store *)

  val find : Vclock.t -> t -> elt option
  (** [find vec key] returns [Some val] if value exists or [None] *)

  val latest: t -> (Vclock.t * elt)
  (** [latest ()] returns the latest version for the replica *)
end

(** Implementaion of versioned store *)
module Make (Type : S.TYPE): STORE with type elt = Type.t = struct
  type elt = Type.t

  let elt = Type.t

  type t = {replica_id : int ; variable_id : string ; servers : Scylla.conn list}
              
  let create_meta_query s = Printf.sprintf
    "create table if not exists mrdt.%s_meta(
    vec blob,
    key blob,
    actor1 int,
    actor2 int,
    ver bigint,
    primary key (vec))" s

  let create_value_query s = Printf.sprintf
    "create table if not exists mrdt.%s_value(
    key blob,
    value blob,
    primary key (key))" s

  let insert_meta_query s = Printf.sprintf
    "insert into mrdt.%s_meta(
    vec,
    key,
    actor1,
    actor2,
    ver)
    VALUES (?,?,?,?,?)" s

  let insert_value_query s = Printf.sprintf
    "insert into mrdt.%s_value(
    key,
    value)
    VALUES (?,?)" s

  let select_meta_query s = Printf.sprintf
    "select key from mrdt.%s_meta where vec = ?" s

  let select_value_query s = Printf.sprintf
    "select value from mrdt.%s_value where key = ?" s

  let select_latest_query1 s r = Printf.sprintf
    "select vec, key, ver from mrdt.%s_meta where actor1 = %d and ver > -1 allow filtering" s r

  let select_latest_query2 s r = Printf.sprintf
    "select vec, key, ver from mrdt.%s_meta where actor2 = %d and ver > -1 allow filtering" s r

  let select_root_query s = Printf.sprintf
    "select vec, key, ver from mrdt.%s_meta where actor1 = -1 and actor2 = -1 allow filtering" s

  let to_bin_string = Irmin.Type.to_bin_string

  let of_bin_string = Irmin.Type.of_bin_string

  let get_blob = function Blob b -> b | _ -> failwith "error"

  let get_value v =
    let v = (function Blob b -> b | _ -> failwith "error") v in
    Bigstringaf.to_string v

  let get_int64 = function Bigint i -> i | _ -> failwith "error"

  let init_val v r =
    let rep = to_bin_string Type.t v in
    let bin = Bytes.of_string rep in
    let hash = Digest.bytes bin in
    let key = Blob (Bigstringaf.of_string hash ~off:0 ~len:(String.length hash)) in
    let actor1 = Int (Int32.of_int r) in
    let actor2 = Int (-1l) in
    let ver = Bigint (0L) in
    let vec = Blob (Vclock.init ()) in
    let value = Bigstringaf.create (Bytes.length bin) in
    Bigstringaf.blit_from_bytes bin ~src_off:0 value ~dst_off:0 ~len:(Bytes.length bin);
    let value = Blob value in
    (key, vec, actor1, actor2, ver, value)

  let make_key_value v =
    let rep = to_bin_string Type.t v in
    let bin = Bytes.of_string rep in
    let hash = Digest.bytes bin in
    let key = Blob (Bigstringaf.of_string hash ~off:0 ~len:(String.length hash)) in
    let value = Bigstringaf.create (Bytes.length bin) in
    Bigstringaf.blit_from_bytes bin ~src_off:0 value ~dst_off:0 ~len:(Bytes.length bin);
    let value = Blob value in
    (key, hash, value)

  let init v r s l =
    let open Result in
    (* create the tables *)
    let conn = List.hd l in
    let _ = query conn ~query:(create_meta_query s) () |> get_ok in
    let _ = query conn ~query:(create_value_query s) () |> get_ok in
    let (key, vec, _, actor2, ver, value) = init_val v r in
    let actor1 = Int (-1l) in
    let _ = query conn ~query:(insert_meta_query s)
      ~values:[|vec ; key ; actor1 ; actor2 ; ver |] () |> get_ok in
    let _ = query conn ~query:(insert_value_query s)
      ~values:[|key ; value |] () |> get_ok in
    {replica_id = r ; variable_id = s ; servers = l}

  let mem vec _key t =
    let vec = Blob vec in
    let conn = List.hd t.servers in
    let res = (query conn ~query:(select_meta_query t.variable_id)
      ~values:[| vec |] () |> Result.get_ok).values in
    Array.length res > 0

  let add vec value ?actor2:(actor2 : int option) t =
    let conn = List.hd t.servers in
    let (key, hash, value) = make_key_value value in
    let actor1 = Int (Int32.of_int t.replica_id) in
    let actor2 = Int ((function Some i -> Int32.of_int i | None -> -1l) actor2) in
    let ver = Bigint (Vclock.sum vec) in
    let vec = Blob vec in
    let _ = query conn ~query:(insert_value_query t.variable_id)
        ~values:[|key ; value|] () |> Result.get_ok in
    let _ = query conn ~query:(insert_meta_query t.variable_id)
        ~values:[|vec ; key ; actor1 ; actor2 ; ver |] () |> Result.get_ok in
    hash

  let find vec t =
    let conn = List.hd t.servers in
    let vec = Blob vec in
    let res = (query conn ~query:(select_meta_query t.variable_id)
      ~values:[| vec |] () |> Result.get_ok).values in
    if (Array.length res = 0) then begin
      print_endline "empty";
      None
    end else begin
      let key = res.(0).(0) in
      let value = get_value (query conn ~query:(select_value_query t.variable_id)
        ~values:[|key|] () |> Result.get_ok).values.(0).(0) in
      let value = of_bin_string Type.t value |> Result.get_ok in
      Some value
    end

  let latest t =
    let conn = List.hd t.servers in
    let values1 = (query conn
      ~query:(select_latest_query1 t.variable_id t.replica_id) () |> Result.get_ok).values in
    let values2 = (query conn
      ~query:(select_latest_query2 t.variable_id t.replica_id) () |> Result.get_ok).values in
    let values = Array.concat [values1 ; values2] in
    let values = if (Array.length values) = 0 then
      (query conn ~query:(select_root_query t.variable_id) () |> Result.get_ok).values
      else values
    in
    let values = Array.map (fun i -> (get_blob i.(0) , get_value i.(1) , get_int64 i.(2), i.(1))) values in
    let max_ver = ref (-1L) in
    let max_ind = ref (-1) in
    for i = 0 to (Array.length values) - 1 do
      let (_, _, ver, _) = values.(i) in
      if (ver > !max_ver) then begin
        max_ver := ver;
        max_ind := i
      end else
        ()
    done;
    let (vec, _key, _, kblob) = values.(!max_ind) in
    let value = get_value (query conn ~query:(select_value_query t.variable_id) ~values:[|kblob|] () |> Result.get_ok).values.(0).(0) in
    let value = Irmin.Type.of_bin_string elt value |> Result.get_ok in
    (vec, value)
end
