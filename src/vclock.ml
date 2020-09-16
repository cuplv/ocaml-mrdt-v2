(* #TODO: hardcoded for 32 replicas (8 * 32) bytes *)
open Bigstringaf
   
let k = 32

let n = 8 * k

type t = Bigstringaf.t

let init _ =
  let v = create n in
  for i = 0 to k - 1 do
    set_int64_be v (i*8) 0L
  done;
  v

let incr i t =
  if (i > k) then
    failwith "replica index greater than number of replicas"
  else begin
    let v = get_int64_be t (i*8) in
    let nv = Int64.add v 1L in
    set_int64_be t (i*8) nv
  end

let merge t1 t2 =
  for i = 0 to k - 1 do
    let v1 = get_int64_be t1 (i*8) in
    let v2 = get_int64_be t2 (i*8) in
    let nv = match (Int64.compare v1 v2) with
      | -1 -> v2
      | _ -> v1 in
    set_int64_be t1 (i*8) nv;
    set_int64_be t2 (i*8) nv;
  done

let compare_at i t1 t2 =
  let open Int64 in
  compare (get_int64_be t1 (i * 8)) (get_int64_be t2 (i*8))

let compare t1 t2 =
  let open List in
  let indices = init k (fun i -> i) in
  let compares = map (fun i -> compare_at i t1 t2) indices in
  let t2_over_t1 = for_all (fun x -> x = -1 || x = 0) compares in
  let t1_over_t2 = for_all (fun x -> x = 1 || x = 0) compares in
  let equal = for_all (fun x -> x = 0) compares in
  match t2_over_t1, t1_over_t2, equal with
  | _, _, true -> 0
  | false, false, _ -> 2
  | true, false, _ -> -1
  | false, true, _ -> 1
  | _, _, _ -> failwith "unknown comparison"

let lca t1 t2 =
  let t = create n in
  let indices = List.init k (fun i -> i) in
  let set_at i =
    match compare_at i t1 t2 with
    | -1 -> get_int64_be t1 (i*8)
    | _ -> get_int64_be t2 (i*8) in
  List.iter (fun i -> set_int64_be t (i * 8) (set_at i)) indices;
  t

let sum t =
  let index = ref 0L in
  for i = 0 to k - 1 do
    index := Int64.add !index (Bigstringaf.get_int64_be t (i*8))
  done;
  !index
