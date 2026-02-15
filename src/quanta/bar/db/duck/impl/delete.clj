(ns quanta.bar.db.duck.impl.delete
  (:require
   [tmducken.duckdb :as duckdb]
   [quanta.bar.db.duck.impl.calendar :refer [bar-category->table-name]]))

(defn sql-delete-bars-asset [calendar asset]
  (let [table-name (bar-category->table-name calendar)]
    (str "delete from " table-name
         " where asset = '" asset "'")))

(defn delete-bars [conn calendar asset]
  (duckdb/run-query!
   conn
   (sql-delete-bars-asset calendar asset)))

(defn sql-delete-bars-calendar [calendar]
  (let [table-name (bar-category->table-name calendar)]
    (str "delete from " table-name)))

(defn delete-calendar [conn calendar]
  (duckdb/run-query!
   conn
   (sql-delete-bars-calendar calendar)))

(defn sql-delete-bars-asset-dt [calendar asset dt]
  (let [table-name (bar-category->table-name calendar)]
    (str "delete from " table-name
         " where asset = '" asset "' and date = '" dt "'")))

(defn delete-bars-asset-dt [conn calendar asset dt]
  (duckdb/run-query!
   conn
   (sql-delete-bars-asset-dt calendar asset dt)))