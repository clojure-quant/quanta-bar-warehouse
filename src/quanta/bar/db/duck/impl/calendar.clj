(ns quanta.bar.db.duck.impl.calendar)

(defn interval-safe [i]
  ; this is here because duckdb tables dont differentiate
  ; between small cap and big cap.
  (case i
    :Y :yy
    :M :mm
    :W :ww
    :D :dd
    i))

(defn bar-category->table-name [[market interval]]
  (str (name market) "_" (name (interval-safe interval))))

(comment

  (bar-category->table-name [:us :m])

 ; 
  )