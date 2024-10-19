(ns dev.preload
  (:require
   [missionary.core :as m]
   [tick.core :as t]
   [quanta.bar.preload :refer [import-bars import-bars-one]]
   [dev.env :refer [bar-engine]]))

;; test the import-one routine 
;; this should not be used by users

(m/? (import-bars-one
      bar-engine
      {:asset "EUR/USD" :calendar [:us :d]
       :import :kibot
       :to :nippy
       :window {:start (t/instant "2000-01-01T00:00:00Z")
                :end (t/instant "2024-09-10T00:00:00Z")}
       :label "test"}))

;; bybit

(m/?
 (import-bars
  bar-engine
  {:asset ["BTCUSDT" "ETHUSDT"]
   :parallel-nr 100
   :calendar [:crypto :d]
   :import :bybit-parallel
   :to :nippy
   :window {:start (t/instant "2000-01-01T00:00:00Z")
            :end (t/instant "2024-09-10T00:00:00Z")}
   :label "test"}))

;; kibot
;; note that assets have to be in asset-db

(import-bars
 bar-engine
 {:asset ["EUR/USD" "USD/JPY"] :calendar [:us :d]
  :parallel-nr 100
  :import :kibot
  :to :nippy
  :window {:start (t/instant "2000-01-01T00:00:00Z")
           :end (t/instant "2024-09-10T00:00:00Z")}
  :label "multiple forex quotes test"})

