(ns dev.bardb.duck-performance
  (:require
   [missionary.core :as m]
   [tablecloth.api :as tc]
   [tick.core :as t]
   [quanta.bar.protocol :as b]
   [dev.bardb.duck-generator :refer [db generate-bars-assets load-bars-assets]]))

;; BUG: this duplicates the generated bars!

(defn generate-eod [db-name asset-n]
  (generate-bars-assets
   {:asset-n asset-n
    :label db-name
    :calendar [:us :d]
    :start (t/instant "2005-01-01T00:00:00Z")
    :end  (t/instant "2025-03-01T20:00:00Z")}))

(time (m/? (generate-eod "eod-50-assets" 50)))
(time (m/? (generate-eod "eod-10000-assets" 10000)))
;           MB    Elapsed time  bars 
; 50:       17MB  1.0 secs      50 * 5000 bars
; 10000     1.9G  157 secs      10000 * 5000 bars 

(m/? (b/summary (db "eod-50-assets") {:calendar [:us :d]}))
(m/? (b/summary (db "eod-10000-assets") {:calendar [:us :d]}))

(defn load-eod [db-name asset-n]
  (time
   (m/?
    (load-bars-assets
     {:asset-n asset-n
      :label db-name
      :calendar [:us :d]}))))

(load-eod  "eod-50-assets" 2)
"eod-50-assets"
; "Elapsed time: 211.567646 msecs"
{:asset-n 2, :bar-n 21040}

(load-eod  "eod-50-assets" 50)
; "Elapsed time: 432.872213 msecs"
{:asset-n 50, :bar-n 526000}

(load-eod  "eod-10000-assets" 1000)
; "Elapsed time: 17015.613497 msecs"
; {:asset-n 1000, :bar-n 5328380}

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
