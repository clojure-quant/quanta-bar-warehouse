(ns quanta.bar.db.duck.append-bars
  (:require
   [taoensso.timbre :as timbre :refer [info warn error]]
   [tablecloth.api :as tc]
   [tmducken.duckdb :as duckdb]
   [quanta.bar.db.duck.calendar :refer [bar-category->table-name]]
   [quanta.bar.db.duck.ds :refer [sanitize-ds]]))

(defn append-bars [session {:keys [calendar asset]} ds]
  (let [ds (sanitize-ds ds asset)
        table-name (bar-category->table-name calendar)
        ds-with-name (tc/set-dataset-name ds table-name)]
    (info "ds: " ds)
    (info "duckdb append-bars asset: " asset " calendar: " calendar " bar:# " (tc/row-count ds))
    (duckdb/insert-dataset! (:conn session) ds-with-name)))