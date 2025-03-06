(ns quanta.bar.db.duck.warehouse
  (:require
   [tmducken.duckdb :as duckdb]
   [tablecloth.api :as tc]
   [quanta.bar.db.duck.calendar :refer [bar-category->table-name]]))

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

(defn get-data-range [session calendar asset]
  (-> (:conn session)
      (duckdb/sql->dataset
       (sql-query-warehouse-asset calendar asset))
      (tc/set-dataset-name (str "warehouse " calendar " " asset))))

(defn warehouse-summary [session calendar]
  (let [ds    (duckdb/sql->dataset
               (:conn session)
               (sql-query-warehouse calendar))]
    (when ds
      (-> ds
          (tc/set-dataset-name (str "warehouse " calendar))
          convert-columns-to-keywords))))

