(ns quanta.bar.db.duck.delete
  (:require
   [tmducken.duckdb :as duckdb]
   [quanta.bar.db.duck.calendar :refer [bar-category->table-name]]))

(defn sql-delete-bars-asset [session calendar asset]
  (let [table-name (bar-category->table-name calendar)]
    (str "delete from " table-name
         " where asset = '" asset "'")))

(defn delete-bars [session calendar asset]
  (duckdb/run-query!
   (:conn session)
   (sql-delete-bars-asset session calendar asset)))

(defn sql-delete-bars-calendar [session calendar]
  (let [table-name (bar-category->table-name calendar)]
    (str "delete from " table-name)))

(defn delete-calendar [session calendar]
  (duckdb/run-query!
   (:conn session)
   (sql-delete-bars-calendar session calendar)))