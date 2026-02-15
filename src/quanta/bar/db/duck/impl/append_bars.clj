(ns quanta.bar.db.duck.impl.append-bars
  (:require
   [tablecloth.api :as tc]
   [tmducken.duckdb :as duckdb]
   [quanta.bar.db.duck.impl.calendar :refer [bar-category->table-name]]
   [quanta.bar.db.duck.impl.ds :refer [sanitize-ds]]))

(defn append-bars [conn {:keys [calendar asset]} ds]
  (let [ds (sanitize-ds ds asset)
        table-name (bar-category->table-name calendar)
        ds-with-name (tc/set-dataset-name ds table-name)]
    (duckdb/insert-dataset! conn ds-with-name)))