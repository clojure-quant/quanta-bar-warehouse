(ns dev.bardb.duck-pool
  (:require
   [missionary.core :as m]
   [tmducken.duckdb :as tmdb]
   [quanta.bar.db.duck.warehouse :as wh]
   [quanta.bar.db.duck.get-bars :refer [get-bars]]
   [quanta.bar.db.duck :as duck]
   [quanta.bar.db.duck.pool :refer [start-pool-actor]]
   ))

(def db (duck/start-bardb-duck "./duck-perf/test.ddb"))

(wh/warehouse-summary db [:us :d])

(def pool
  (start-pool-actor
   {:db (:db db)
    :size 3
    :on-create (fn [c]
                 (println "conn created")
                  ;; strongly consider this if you run many concurrent queries:
                  ;; (duckdb-exec c "PRAGMA threads=1")
                 )}))

(try
  (m/?
   (m/sp
    (let [c (m/? ((:acquire pool)))]
      (try
        (println "rcvd conn: " c)
        (let [bar-ds (get-bars c {:asset "BONGO"} {:calendar [:us :d]})]
          (println "bars rcvd: " bar-ds))
        (finally
          (println "release conn after bars rcvd.")
          ((:release pool) c)
          )))))
(catch Exception ex
   (println "ex: " ex)
   (println "ex-cause: " (ex-cause ex))
   (println "ex-data: " (ex-data ex))))
