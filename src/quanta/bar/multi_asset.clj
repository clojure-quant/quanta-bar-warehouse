(ns quanta.bar.multi-asset
  (:require
   [taoensso.timbre :as timbre :refer [debug info warn error]]
   [missionary.core :as m]
   [quanta.bar.protocol :as b]))

(defn get-bars-assets
  "returns a map of key asset and value bar-ds
   opts :calendar :asset [vec of asset-symbols]
   returns a map of key asset and value bar-ds"
  [bardb opts window]
  (let [assets (:asset opts)
        load-asset (fn [asset]
                     (m/sp
                      (let [ds (m/? (b/get-bars bardb (assoc opts :asset asset) window))]
                        [asset ds])))
        tasks (map load-asset assets)
        result-fn (fn [& results]
                    (info "results: " results)
                    (into {} results))]
    (apply m/join result-fn tasks)))