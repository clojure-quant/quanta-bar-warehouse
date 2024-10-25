(ns quanta.bar.transform.helper
  (:require
    [taoensso.timbre :as timbre :refer [debug info warn error]]))

(defn get-source-interval [interval-config interval]
  (let [source-interval (get interval-config interval)]
    (info "requested interval: " interval "source interval: " source-interval)
    source-interval))
