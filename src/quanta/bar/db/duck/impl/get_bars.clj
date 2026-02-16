(ns quanta.bar.db.duck.impl.get-bars
  (:require
   [clojure.string]
   [tick.core :as t]
   [tablecloth.api :as tc]
   [tmducken.duckdb :as duckdb]
   [quanta.bar.db.duck.impl.calendar :refer [bar-category->table-name]]
   [quanta.bar.db.duck.impl.ds :refer [empty-ds]]))

;; SQL  helper

(defn sql-select-table [calendar]
  (let [table-name (bar-category->table-name calendar)]
    (str "SELECT * from " table-name)))

(defn sql-where-asset [asset multiple?]
  (if multiple?
    (str " WHERE asset IN ("
         (clojure.string/join "," (map #(str "'" % "'") asset))
         ")")
    (str " WHERE asset = '" asset "'")))

(defn sql-date-since [since]
  (str " and date >= '" since "'"))

(defn sql-date-until [until]
  (str " and date <= '" until "'"))

(defn sql-date-range [dstart dend]
  (str " and date >= '" dstart "' and date <= '" dend "'"))

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

(defn bar-query [conn sql]
  (-> (duckdb/sql->dataset conn sql)
      (keywordize-columns)))

(defn asset-multiple? [asset]
  ;(or (vector? asset) (seq? asset) (set? asset))
  (vector? asset))

;; full sql/queries

(defn sql-bars-full [calendar asset multiple?]
  (str (sql-select-table calendar)
       (sql-where-asset asset multiple?)
       (if multiple? "" " order by date")))

(defn get-bars-full [conn calendar asset]
  (let [multiple? (asset-multiple? asset)]
    (bar-query conn (sql-bars-full calendar asset multiple?))))

(defn sql-bars-window [calendar asset multiple? dstart dend]
  (str (sql-select-table calendar)
       (sql-where-asset asset multiple?)
       (sql-date-range dstart dend)
       (if multiple? "" " order by date")))

(defn get-bars-window [conn calendar asset dstart dend]
  (let [multiple? (asset-multiple? asset)]
    (bar-query conn (sql-bars-window calendar asset multiple? dstart dend))))

(defn sql-bars-since [calendar asset multiple? since]
  (str (sql-select-table calendar)
       (sql-where-asset asset multiple?)
       (sql-date-since since)
       (if multiple? "" " order by date")))

(defn get-bars-since [conn calendar asset since]
  (let [multiple? (asset-multiple? asset)]
    (bar-query conn (sql-bars-since calendar asset multiple? since))))

;; UNTIL 

(defn sql-bars-until [calendar asset multiple? until]
  (str (sql-select-table calendar)
       (sql-where-asset asset multiple?)
       (sql-date-until until)
       (if multiple? "" " order by date")))

(defn sql-bars-until-n [calendar asset multiple? until n]
    ; this does only support single asset.
  (str (sql-select-table calendar)
       (sql-where-asset asset multiple?)
       (sql-date-until until)
       " order by date desc"
       " limit " n))

(defn get-bars-until
  ([conn calendar asset until]
   (let [multiple? (asset-multiple? asset)]
     (bar-query conn (sql-bars-until calendar asset multiple? until))))
  ([conn calendar asset until n]
   (if (asset-multiple? asset)
     (throw (ex-info "get-bars-until [n] only supports single asset!" {:calendar calendar
                                                                       :asset asset
                                                                       :until until
                                                                       :n n}))
     (-> (bar-query conn (sql-bars-until-n calendar asset false until n))
         (tc/order-by :date)))))

; 

(defn ensure-instant [dt]
  (when dt
    (if (t/instant? dt)
      dt
      (t/instant dt))))

(defn get-bars
  "returns bar-ds for asset/calendar + window"
  [conn {:keys [asset] :as opts} {:keys [calendar start end n] :as window}]
  (try
    (let [;tmlducken v0.10 cannot do queries with date being zoned-datetime
          start (ensure-instant start)
          end (ensure-instant end)
          calendar (or calendar (:calendar opts))
          bar-ds (cond
                   ; window - start-end 
                   (and start end)
                   (get-bars-window conn calendar asset start end)

                   ; since - starting >>
                   start
                   (get-bars-since conn calendar asset start)

                    ; until -limit end 
                   (and end n)
                   (get-bars-until conn calendar asset end n)

                     ; until full
                   end
                   (get-bars-until conn calendar asset end)

                   ; full - entire history
                   :else
                   (get-bars-full conn calendar asset))]
      (cond
        (nil? bar-ds)
        empty-ds

        :else
        bar-ds))
    (catch Exception ex
      (throw (ex-info "get-bars duckdb" {:window window
                                         :opts opts
                                         :ex ex})))))