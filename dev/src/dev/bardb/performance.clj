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


(defn generate-bars [{:keys [bardb rand]} {:keys [asset calendar start end]}]
  (m/sp
   
   (let [bar-ds (m/? (b/get-bars rand
                                 {:asset asset
                                  :calendar calendar}
                                 {:start start
                                  :end end}))]
     (m/? (b/append-bars bardb {:asset asset
                                :calendar calendar}
                         bar-ds)))))

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


(defn db [label]
  (duck/start-bardb-duck (str "./duck-perf/" label ".ddb ")))

(def alphabet "abcdefghijklmnopqrstuvwxyz")

(defn int->str5 [n]
  (let [r (java.util.Random. (long n))]
    (apply str
           (repeatedly 5
                       #(nth alphabet (.nextInt r (count alphabet)))))))


(defn generate-bars-assets [ctx {:keys [asset-n label] :as opts}]
  (let [opts (dissoc opts :asset-n :label)
        bardb (db label)
        ctx (assoc ctx :bardb bardb)]
    (m/sp
     (->>  (range asset-n)
           (map int->str5)
           (map (fn [asset]
                  (m/? (generate-bars ctx (assoc opts :asset asset)))
                  nil))
           doall)
    (checkpoint bardb)
    (duck/stop-bardb-duck bardb)
    (str "generated bars for " asset-n "assets in db: " label)
    )))

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

(defn load-bars-assets [{:keys [asset-n calendar label]}]
    (println "load-bars outside thread: " (.getId (Thread/currentThread)))
    (m/sp
     (println "load-bars inside thread: " (.getId (Thread/currentThread)))
     (let [bardb (-> (db label)
                     (with-new-conn))
           row-count (->>  (range asset-n)
                           (map int->str5)
                           (map (fn [asset]
                                  (println "q asset: " asset " calendar: " calendar)
                                  (let [bar-ds (m/? (b/get-bars bardb {:asset asset :calendar calendar} {}))]
                                    (println "bar-ds: " bar-ds)
                                    ;(tc/row-count bar-ds)
                                    0
                                    )))
                           (reduce + 0.0)
                           )]
       (duck/stop-bardb-duck bardb)
       ;(str "loaded bars for " asset-n "assets in db: " label)
       row-count)))

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