(ns ta.db.bars.duckdb.delete
  (:require
   [tmducken.duckdb :as duckdb]
   [ta.db.bars.duckdb.calendar :refer [bar-category->table-name]]))

(defn sql-delete-bars-asset [session calendar asset]
  (let [table-name (bar-category->table-name calendar)]
    (str "delete from " table-name
         " where asset = '" asset "'")))

(defn delete-bars [session calendar asset]
  (duckdb/run-query!
   (:conn session)
   (sql-delete-bars-asset session calendar asset)))