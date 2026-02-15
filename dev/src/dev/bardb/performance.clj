(ns dev.bardb.performance
  (:require
   [missionary.core :as m]
   [tick.core :as t]
   [tmducken.duckdb :as tmduck]
   [quanta.bar.protocol :as b]
   [quanta.bar.db.duck :as duck]
   [quanta.bar.db.duck.warehouse :as wh]
   [quanta.bar.db.duck.admin :refer [checkpoint query]]
   [dev.env :refer [rs]]
   [tablecloth.api :as tc]))


(def bardb (duck/start-bardb-duck "./duck-perf/test.ddb"))

(m/? (generate-bars
      {:bardb bardb :rand rs}
      {:asset "BONGO"
       :calendar [:us :d]
       :start (t/instant "2005-01-01T00:00:00Z")
       :end  (t/instant "2010-03-01T20:00:00Z")}))

(wh/warehouse-summary bardb [:us :d])

(m/? (b/get-bars bardb  {:asset "BONGO" :calendar [:us :d]} {}))

(checkpoint bardb)

(duck/stop-bardb-duck bardb)


(query bardb "SELECT schema_name FROM information_schema.schemata;")
(query bardb "PRAGMA show_tables;")

(query bardb "SELECT  column_name, data_type, is_nullable
              FROM information_schema.columns
              WHERE table_name = 'crypto_d'
              ORDER BY ordinal_position;")

(query bardb "CREATE TABLE prices_sorted AS
              SELECT * FROM crypto_d
              ORDER BY asset, date;")

(query bardb "SELECT column_name, data_type, is_nullable
              FROM information_schema.columns
              WHERE table_name = 'prices_sorted'
              ORDER BY ordinal_position;")

(query bardb "PRAGMA table_info('crypto_d');")

;; BUG: this duplicates the generated bars!



(time (m/? (generate-bars-assets
            {:bardb bardb :rand rs}
            {:asset-n 10000
             :label "10000-assets-daily"
             :calendar [:us :d]
             :start (t/instant "2005-01-01T00:00:00Z")
             :end  (t/instant "2025-03-01T20:00:00Z")})))
;           MB    Elapsed time  bars 
; 50:       17MB  1.2 secs      50 * 5000 bars
; 10000     1.9G  157 secs      10000 * 5000 bars 

(wh/warehouse-summary (db "50-assets-daily") [:us :d])
(wh/warehouse-summary (db "10000-assets-daily") [:us :d])

(defn with-new-conn [bardb]
  (assoc bardb :conn (tmduck/connect (:db bardb))))



(try
 (m/? (load-bars-assets
       {:asset-n 1 ; 10000
        :label "50-assets-daily";"10000-assets-daily"
        :calendar [:us :d]}))
   (catch Exception ex
     (println "ex: " ex)
     (println "ex-cause: " (ex-cause ex))
     (println "ex-data: " (ex-data ex))))

(with-new-conn (db "50-assets-daily"))  


(try (m/? (b/get-bars (with-new-conn (db "50-assets-daily"))
                      {:asset (int->str5 0)
                       :calendar [:us :d]} {}))
(catch Exception ex
   (println "ex: " ex)
   (println "ex-cause: " (ex-cause ex))
   (println "ex-data: " (ex-data ex))))

(try (m/? (m/join vector
                  (b/get-bars (with-new-conn (db "50-assets-daily"))
                              {:asset (int->str5 0)
                               :calendar [:us :d]} {})
                  (m/sleep 1000 42)
                  (b/get-bars (with-new-conn (db "50-assets-daily"))
                              {:asset (int->str5 1)
                               :calendar [:us :d]} {})
           
           ))
     (catch Exception ex
       (println "ex: " ex)
       (println "ex-cause: " (ex-cause ex))
       (println "ex-data: " (ex-data ex))))


(m/? (b/get-bars (db "10000-assets-daily")
                 {:asset (int->str5 0)
                  :calendar [:us :d]} {}))


5260

(defn create-partition-table [conn table-name from to]
  (format " CREATE TABLE %s PARTITION OF %s
                                          FOR VALUES FROM ('%s') TO ('%s');",
          (str table-name "_" from)
          table-name
          from
          to))

(defn create-hive-partition-by-asset [conn table-name path]
  (str "COPY " table-name "
          TO '" path "' (FORMAT PARQUET, PARTITION_BY (asset));"))

(comment
; create databases
  (create-new-db base-path {:assets 1 :years 1} [:crypto :m])     ; 12 MB
  (create-new-db base-path {:assets 100 :years 10} [:crypto :m])  ; 11 GB

  (create-new-db base-path {:assets 1 :years 10} [:crypto :m])    ; 110 MB
  (create-new-db base-path {:assets 1 :years 100} [:crypto :m])   ; 1.1 GB

  (create-new-db base-path {:assets 10 :years 1} [:crypto :m])    ; 114 MB
  (create-new-db base-path {:assets 100 :years 1} [:crypto :m])   ; 1.1 GB
  (create-new-db base-path {:assets 1000 :years 1} [:crypto :m])  ; 11 GB

  (defn performance-test
    [_]
    (time (series-generate-save-reload 2000 "small"))
    (time (series-generate-save-reload 20000 "big")) ; tradingview limit
    (time (series-generate-save-reload 200000 "huge"))
    (time (series-generate-save-reload 2000000 "gigantic"))))
 ;; results
  ; 500k bars x 1 asset 1y = 1.628 | 2.597 sec
  ; 500k bars x 10 asset 1y =  1.5 | 1.7 sec
  ; 500k bars x 1000 asset 1y = 2.0 | 2.3 | 3.2 sec

  ; 5mio  bars x 1 asset 10y = 16.71 sec
  ; 50mio bars x 1 asset 100y = 274.860 sec