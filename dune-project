(lang dune 2.7)
(name mrdt)
(version 0.0.1)

(generate_opam_files true)

(source (github anmolsahoo25/mrdt))
(license MIT)
(authors "Anmol Sahoo")
(maintainers "Anmol Sahoo<anmol.sahoo25@gmail.com>")

(package
  (name mrdt)
  (synopsis "Mergeable replicated data types")
  (depends
    (dune (> 2.0.0))
    (irmin (> 2.0.0))
    (ppx_irmin (> 2.0.0))))
