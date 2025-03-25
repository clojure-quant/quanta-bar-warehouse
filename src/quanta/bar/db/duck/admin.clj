(ns quanta.bar.db.duck.admin
  (:require
   [taoensso.timbre :as timbre :refer [debug info warn error]]
   [tmducken.duckdb :as duckdb]))

(defn checkpoint [session]
  (duckdb/run-query!  (:conn session) "CHECKPOINT;"))

(defn db-size [session]
  (duckdb/sql->dataset (:conn session) "PRAGMA database_size;"))

