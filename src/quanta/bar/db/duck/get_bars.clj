(ns quanta.bar.db.duck.get-bars
  (:require
   [taoensso.timbre :as timbre :refer [debug info error]]
   [tick.core :as t]
   [tablecloth.api :as tc]
   [tmducken.duckdb :as duckdb]
   [quanta.bar.db.duck.calendar :refer [bar-category->table-name]]
   [quanta.bar.db.duck.ds :refer [empty-ds]]))

(defn keywordize-columns [ds]
  (tc/rename-columns
   ds
   {"date" :date
    "open" :open
    "high" :high
    "low" :low
    "close" :close
    "volume" :volume
    "asset" :asset
    "ticks" :ticks}))

(defn sql-query-bars-for-asset [calendar asset]
  (let [table-name (bar-category->table-name calendar)]
    (str "select * from " table-name " where asset = '" asset "' order by date")))

(defn get-bars-full [session calendar asset]
  (debug "get-bars " asset)
  (let [query (sql-query-bars-for-asset calendar asset)]
    (-> (duckdb/sql->dataset (:conn session) query)
        (keywordize-columns))))

(defn sql-query-bars-for-asset-since [calendar asset since]
  (let [table-name (bar-category->table-name calendar)]
    (str "select * from " table-name
         " where asset = '" asset "'"
         " and date > '" since "'"
         " order by date")))

(defn get-bars-since [session calendar asset since]
  (debug "get-bars-since " asset since)
  (let [query (sql-query-bars-for-asset-since calendar asset since)]
    (-> (duckdb/sql->dataset (:conn session) query)
        (keywordize-columns))))

(defn sql-query-bars-for-asset-until 
  ([calendar asset until]
   (let [table-name (bar-category->table-name calendar)]
     (str "select * from " table-name
          " where asset = '" asset "'"
          " and date <= '" until "'"
          " order by date")))
  ([calendar asset until n]
   (let [table-name (bar-category->table-name calendar)]
     (str "select * from " table-name
          " where asset = '" asset "'"
          " and date <= '" until "'"
          " order by date desc"
          " limit " n))))

(defn get-bars-until 
  ([session calendar asset until]
   (debug "get-bars-until " asset until)
   (let [query (sql-query-bars-for-asset-until calendar asset until)]
     (-> (duckdb/sql->dataset (:conn session) query)
         (keywordize-columns))))
  ([session calendar asset until n]
  (debug "get-bars-until " asset until n)
  (let [query (sql-query-bars-for-asset-until calendar asset until n)]
    (-> (duckdb/sql->dataset (:conn session) query)
        (keywordize-columns)
        (tc/order-by :date)
        ))))


(defn sql-query-bars-for-asset-window [calendar asset dstart dend]
  (let [table-name (bar-category->table-name calendar)]
    (str "select * from " table-name
         " where asset = '" asset "'"
         " and date >= '" dstart "'"
         " and date <= '" dend "'"
         " order by date")))

(defn get-bars-window [session calendar asset dstart dend]
  (debug "get-bars-window " asset dstart dend)
  (let [query (sql-query-bars-for-asset-window calendar asset dstart dend)]
    (debug "sql-query: " query)
    (-> (duckdb/sql->dataset (:conn session) query)
        (keywordize-columns))))

(defn ensure-instant [dt]
  (when dt
    (if (t/instant? dt)
      dt
      (t/instant dt))))

(defn get-bars
  "returns bar-ds for asset/calendar + window"
  [session {:keys [asset] :as opts} {:keys [calendar start end n] :as window}]
  (try
    (let [;tmlducken v0.10 cannot do queries with date being zoned-datetime
          start (ensure-instant start)
          end (ensure-instant end)
          calendar (or calendar (:calendar opts))
          bar-ds (cond
                   ; start-end window
                   (and start end)
                   (get-bars-window session calendar asset start end)

                   ; starting >>
                   start
                   (get-bars-since session calendar asset start)

                   
                   ; end 
                   (and end n)
                   (get-bars-until session calendar asset end n)

                   end
                   (get-bars-until session calendar asset end)
                   
                   ; entire history
                   :else
                   (get-bars-full session calendar asset))]
      (cond
        (nil? bar-ds)
        empty-ds

        :else
        bar-ds))
    (catch Exception ex
      (error "get-bars "  opts 
             " window: " window 
             "exception: " ex)
      (throw (ex-info "get-bars duckdb" {:window window
                                         :opts opts})))))