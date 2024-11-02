(ns dev.trailing
  (:require
   [taoensso.timbre :refer [trace debug info warn error]]
   [missionary.core :as m]
   [tick.core :as t]
   [quanta.bar.env]
   [dev.env :refer [bar-engine]]))

bar-engine

(try
  (info "getting bars test..")
  (with-bindings {#'quanta.bar.env/*bar-db* bar-engine}
    (m/?
     (quanta.bar.env/get-bars
      {:trailing-n 100
       :calendar [:crypto :d]
       :import :bybit-parallel
       :asset "BTCUSDT"}
      {:start  (t/instant "2024-05-01T00:00:00Z")
       :end (t/instant "2024-07-01T00:00:00Z")})))
  (catch Exception ex
    (info "ex: " ex))
  (catch AssertionError ae
    (info "ae: " ae)))

(with-bindings {#'quanta.bar.env/*bar-db* bar-engine}
  (m/?
   (quanta.bar.env/get-trailing-bars
    {:trailing-n 100
     :calendar [:crypto :d]
     :import :bybit-parallel
     :asset "BTCUSDT"}
    (t/instant))))



