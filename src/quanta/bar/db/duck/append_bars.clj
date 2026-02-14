(ns quanta.bar.db.duck.append-bars
  (:require
   [tablecloth.api :as tc]
   [tmducken.duckdb :as duckdb]
   [quanta.bar.db.duck.calendar :refer [bar-category->table-name]]
   [quanta.bar.db.duck.ds :refer [sanitize-ds]]))

(defn append-bars [session {:keys [calendar asset]} ds]
  (let [ds (sanitize-ds ds asset)
        table-name (bar-category->table-name calendar)
        ds-with-name (tc/set-dataset-name ds table-name)]
    (duckdb/insert-dataset! (:conn session) ds-with-name)))