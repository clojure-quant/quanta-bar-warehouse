(ns quanta.bar.db.nippy
  (:require
   [clojure.string :as str]
   [missionary.core :as m]
   [taoensso.timbre :as timbre :refer [debug info error]]
   [tablecloth.api :as tc]
   [tick.core :as t]
   ;[clojure.java.io :as java-io]
   [tech.v3.io :as io]
   [babashka.fs :refer [create-dirs]]
   [quanta.bar.protocol :refer [bardb barsource]])
  (:import (java.io FileNotFoundException)))

(defn save-ds [filename ds]
  (let [s (io/gzip-output-stream! filename)]
    (debug "saving series " filename " count: " (tc/row-count ds))
    (io/put-nippy! s ds)))

(defn load-ds [filename]
  (let [s (io/gzip-input-stream filename)
        ds (io/get-nippy s)]
    (debug "loaded series " filename " count: " (tc/row-count ds))
    ds))

(defn append-ds [filename ds]
  (let [existing-ds (try
                      (load-ds filename)
                      (catch FileNotFoundException _ex
                        (tc/dataset [])))]
    (debug "appending series " filename " count:" (tc/row-count ds) ", existing:" (tc/row-count existing-ds))
    (save-ds filename (tc/concat existing-ds ds))))

(defn filename-asset [this {:keys [asset calendar]}]
  (let [[exchange interval] calendar
        asset (str/replace asset #"/" "_")]
    (str (:base-path this) asset "-" (name exchange) "-" (name interval) ".nippy.gz")))

(defn filter-range [ds-bars {:keys [start end]}]
  (tc/select-rows
   ds-bars
   (fn [row]
     (let [date (:date row)]
       (and
        (or (not start) (t/>= date start))
        (or (not end) (t/<= date end)))))))

(defn get-bars-nippy [this opts window]
  (info "get-bars " opts window)
  (-> (load-ds (filename-asset this opts))
      (tc/add-column :asset (:asset opts))
      (filter-range window)))

(defn append-bars-nippy [this opts ds-bars]
  (case (:write-mode opts)
    :append
    (append-ds (filename-asset this opts) ds-bars)

    (save-ds (filename-asset this opts) ds-bars)))

(defrecord bardb-nippy [base-path]
  barsource
  (get-bars [this opts window]
    (m/via m/blk (get-bars-nippy this opts window)))
  bardb
  (append-bars [this opts bar-ds]
    (m/via m/blk (append-bars-nippy this opts bar-ds))))

(defn start-bardb-nippy [base-path]
  (debug "creating dir: " base-path)
  (create-dirs base-path)
  (bardb-nippy. base-path))
