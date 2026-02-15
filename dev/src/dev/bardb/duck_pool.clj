(ns dev.bardb.duck-pool
  (:require
   [clojure.pprint :refer [print-table]]
   [missionary.core :as m]
   [tablecloth.api :as tc]
   [quanta.bar.db.duck.impl.warehouse :as w]
   [quanta.bar.db.duck.impl.admin :as a]
   [quanta.bar.db.duck.impl.get-bars :refer [get-bars]]
   [quanta.bar.db.duck.impl.pool :refer [start-pool-actor with-conn]]
   [dev.bardb.duck-generator :refer [asset-symbol]]))

(def db (dev.bardb.duck-generator/db "50-assets-daily"))

(def pool-1 (start-pool-actor {:db (:db db) :size 1}))
(def pool-8 (start-pool-actor {:db (:db db) :size 8}))
(def pool-16 (start-pool-actor {:db (:db db) :size 16}))

(m/?
 (with-conn pool-1
   (w/warehouse-summary c [:us :d])))

(m/?
 (with-conn pool-1
   (a/checkpoint c)))

(m/?
 (with-conn pool-1
   (a/db-size c)))



(m/?
 (with-conn pool-1
   (a/query c "PRAGMA database_size;")))

(m/?
 (with-conn pool-1
   (a/run! c "CHECKPOINT;")))

(m/?
 (with-conn pool-8
   (get-bars c {:asset (asset-symbol 5)} {:calendar [:us :d]})))

(defn get-asset-barcount [pool asset-n]
  (with-conn pool
    (let [asset (asset-symbol asset-n)
          bar-ds (get-bars c {:asset asset} {:calendar [:us :d]})]
      {:asset-n asset-n :asset asset
       :count (tc/row-count bar-ds)
       :low  (->> bar-ds :close (apply min))
       :high (->> bar-ds :close (apply max))})))

(m/? (get-asset-barcount pool-1 7))

(print-table
 (m/?  (m/join vector
               (get-asset-barcount pool-1 3)
               (get-asset-barcount pool-1 44)
               (get-asset-barcount pool-1 4)
               )))

(time
 (m/?
  (with-conn pool-1
    (a/query c "SELECT asset, date, close
               FROM us_d"))))
; "Elapsed time: 50.935172 msecs"

(time
 (print-table
  (m/?  (apply m/join vector
               (repeatedly 1000 #(get-asset-barcount pool-8 (rand-int 50)))))))
; "Elapsed time: 1126.139369 msecs"

(time
 (print-table
  (m/?  (apply m/join vector
               (repeatedly 1000 #(get-asset-barcount pool-16 (rand-int 50)))))))
; "Elapsed time: 1135.317616 msecs"

(time
 (print-table
  (m/?  (apply m/join vector
               (repeatedly 1000 #(get-asset-barcount pool-1 (rand-int 50)))))))
; "Elapsed time: 4508.965647 msecs"

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
          ((:release pool) c))))))
  (catch Exception ex
    (println "ex: " ex)
    (println "ex-cause: " (ex-cause ex))
    (println "ex-data: " (ex-data ex))))

(macroexpand
 '(with-conn pool
    (get-bars c {:asset "BONGO"} {:calendar [:us :d]})))
 

