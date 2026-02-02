(ns quanta.bar.transform.helper
  (:require
   [tablecloth.api :as tc]
   [quanta.bar.protocol :as b]
   [taoensso.timbre :as timbre :refer [info warn error]]))

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

(defn write-bars [opts bar-ds]
  (assert (contains? opts :to) ":to not set. need target bardb for writing")
  (let [{:keys [engine to asset calendar]} opts
        opts-clean {:asset asset
                    :calendar calendar
                    :bardb to}]
    (info "writing bars:" opts-clean)
    (b/append-bars engine opts-clean bar-ds)))
