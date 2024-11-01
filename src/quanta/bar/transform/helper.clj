(ns quanta.bar.transform.helper
  (:require
   [tablecloth.api :as tc]
   [ta.db.bars.protocol :refer [barsource] :as b]
   [taoensso.timbre :as timbre :refer [debug info warn error]]))

(defn get-source-interval [interval-config interval]
  (let [source-interval (get interval-config interval)]
    (info "requested interval: " interval "source interval: " source-interval)
    source-interval))

(defn get-last-dt [ds]
  (->> (tc/last ds) :date first))

(defn load-stored-bars [opts window]
  (let [engine (:engine opts)
        opts-clean (select-keys opts [:asset :calendar :bardb])]
    (b/get-bars engine opts-clean window)))
