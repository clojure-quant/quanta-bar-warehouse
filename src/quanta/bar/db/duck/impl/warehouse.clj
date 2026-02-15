(ns quanta.bar.db.duck.impl.warehouse
  (:require
   [tmducken.duckdb :as duckdb]
   [tablecloth.api :as tc]
   [quanta.bar.db.duck.impl.calendar :refer [bar-category->table-name]]))

(defn convert-columns-to-keywords [ds]
  (tc/rename-columns ds (zipmap (tc/column-names ds) (map keyword (tc/column-names ds)))))

(defn sql-query-warehouse [calendar]
  (let [table-name (bar-category->table-name calendar)]
    (str "select asset, min(date) as start, "
         "max(date) as end ,"
         "count (*) as count "
         "from " table-name
         " group by asset")))

(defn sql-query-warehouse-asset [calendar asset]
  (let [table-name (bar-category->table-name calendar)]
    (str "select min(date) as start, "
         "max(date) as end ,"
         "count (*) as count "
         "from " table-name
         " where asset = '" asset "'")))

(defn get-data-range [conn calendar asset]
  (-> conn
      (duckdb/sql->dataset
       (sql-query-warehouse-asset calendar asset))
      (tc/set-dataset-name (str "warehouse " calendar " " asset))))

(defn warehouse-summary [conn calendar]
  (let [ds (duckdb/sql->dataset conn (sql-query-warehouse calendar))]
    (when ds
      (-> ds
          (tc/set-dataset-name (str "warehouse " calendar))
          convert-columns-to-keywords
          (tc/order-by :asset)))))

