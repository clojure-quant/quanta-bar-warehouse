(ns quanta.bar.db.duck.impl.admin
  (:require
   [tmducken.duckdb :as duckdb]))

(defn checkpoint [conn]
  (duckdb/run-query! conn "CHECKPOINT;"))

(defn db-size [conn]
  (duckdb/sql->dataset conn "PRAGMA database_size;"))

(defn set-threads [conn n]
  (duckdb/run-query! conn (str "PRAGMA threads=" n ";")))

(defn query [conn sql]
  (duckdb/sql->dataset conn sql))

(defn run! [conn sql]
  (duckdb/run-query! conn sql))

  
