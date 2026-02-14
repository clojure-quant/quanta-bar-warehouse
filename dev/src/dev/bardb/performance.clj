(ns dev.bardb.performance
  (:require
   [missionary.core :as m]
   [tick.core :as t]
   [nano-id.core :refer [nano-id]]
   [quanta.bar.protocol :as b]
   [quanta.bar.db.duck :as duck]
   [quanta.bar.db.duck.warehouse :as wh]
   [dev.env :refer [rs]]))

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

(defn db [label]
  (duck/start-bardb-duck (str "./duck-perf/" label ".ddb ")))

(defn generate-bars-assets [ctx {:keys [asset-n label] :as opts}]
  (let [opts (dissoc opts :asset-n :label)
        bardb (db label)
        ctx (assoc ctx :bardb bardb)]
    (m/sp
     (->> (repeatedly #(nano-id 5))
          (take asset-n)
          (map (fn [asset]
                 (m/? (generate-bars ctx (assoc opts :asset asset)))
                 nil))
          doall)
     (duck/stop-bardb-duck bardb)
     (str "generated bars for " asset-n "assets in db: " label)
     )))

(m/? (generate-bars-assets
      {:bardb bardb :rand rs}
      {:asset-n 50 
       :label "50-assets-daily"
       :calendar [:us :d]
       :start (t/instant "2005-01-01T00:00:00Z")
       :end  (t/instant "2025-03-01T20:00:00Z")}))

(wh/warehouse-summary (db "50-assets-daily") [:us :d])



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