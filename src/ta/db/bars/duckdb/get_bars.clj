(ns ta.db.bars.duckdb.get-bars
  (:require
   [de.otto.nom.core :as nom]
   [taoensso.timbre :as timbre :refer [debug info warn error]]
   [tick.core :as t]
   [tablecloth.api :as tc]
   [tmducken.duckdb :as duckdb]
   [ta.db.bars.duckdb.calendar :refer [bar-category->table-name]]))

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
    "epoch" :epoch
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
  "returns bar-ds for asset/calendar + window
   returns nom anomaly if there are no bars in the dataset."
  [session {:keys [asset calendar] :as opts} {:keys [start end] :as window}]
  (try
    (let [; v0.10 of tmlducken cannot do queries with date
          ; being zoned-datetime
          start (ensure-instant start)
          end (ensure-instant end)
          bar-ds (cond
                   (and start end)
                   (get-bars-window session calendar asset start end)

                   start
                   (get-bars-since session calendar asset start)

                   :else
                   (get-bars-full session calendar asset))]
      (cond
        (or (nil? bar-ds)
            (= 0 (tc/row-count bar-ds)))
        (nom/fail ::get-bars-duckdb {:message (str "asset " asset " has no bars in duckdb!")})

        :else
        bar-ds))
    (catch Exception ex
      (error "get-bars " (select-keys opts [:task-id :asset :calendar :import])
             " window: " (select-keys window [:start :end])
             "exception: " ex)
      (nom/fail ::get-bars-duckdb {:message (str "get-bars asset: " asset
                                                 " calendar: " calendar
                                                 "exception! ")}))))

(comment

  (get-bars db [:us :m] "EUR/USD")
  (get-bars db [:us :m] "ETHUSDT")

  (sql-query-bars-for-asset-since
   [:us :m] "EUR/USD" "2024-01-26T19:35:00Z")

  (require '[tick.core :as t])
  (def dt (t/instant "2024-01-26T19:35:00Z"))
  dt

  (get-bars-since db [:us :m] "EUR/USD" "2024-01-26T19:35:00Z")
  (get-bars-since db [:us :m] "EUR/USD" "2024-01-26 19:35:00")
  (get-bars-since db [:us :m] "EUR/USD" dt)

  (def dt2 (t/instant "2024-01-26T16:35:00Z"))
  (get-bars-since db [:us :m] "EUR/USD" dt2)

  (get-bars-window db [:us :m] "EUR/USD"
                   "2024-01-26T19:35:00Z"
                   "2024-01-26T19:45:00Z")

  (get-bars-window db [:us :m] "ETHUSDT"
                   "2024-01-29T18:56:00Z"
                   "2024-01-29T19:00:00Z")

  (get-bars-window db [:us :m] "ETHUSDT"
                   (t/instant "2024-01-29T18:56:00Z")
                   (t/instant "2024-01-29T19:00:00Z"))

  (get-bars-since db [:us :m] "EUR/USD" time)
  (get-bars-since db [:us :m] "EUR/USD" (str time)))