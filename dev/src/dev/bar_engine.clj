(ns dev.bar-engine
  (:require
   [tick.core :as t]
   [missionary.core :as m]
   [ta.db.bars.protocol :as b]
   [dev.env :refer [bar-engine]]))

(def dt (t/instant "2024-05-01T00:00:00Z"))
dt

;; BYBIT
(m/? (b/get-bars
      bar-engine {:asset "ETHUSDT" ; crypto
                  :calendar [:crypto :m]
                  :import :bybit-parallel}
      {:start  (t/instant "2024-05-01T00:00:00Z")
       :end (t/instant "2024-07-01T00:00:00Z")}))

;; ALPHAVANTAGE
(b/get-bars im {:asset "FMCDX" ; mutual fund
                :calendar [:us :d]
                :import :alphavantage}
            {:start dt
             :mode :append})

(defn date-type [ds]
  (-> ds :date meta :datatype))

;; KIBOT

(b/get-bars im {:asset "NG0" ; future
                :calendar [:us :d]
                :import :kibot}
            {:start  (t/instant "2020-01-01T00:00:00Z")
             :end (t/instant "2024-01-01T00:00:00Z")})

(b/get-bars im
            {:asset "EUR/USD" ; forex
             :calendar [:forex :d]
             :import :kibot}
            {:start (t/instant "2023-09-01T00:00:00Z")
             :end (t/instant "2024-05-01T00:00:00Z")})

(b/get-bars im {:asset "EU0" ; future(forex)
                :calendar [:us :d]
                :import :kibot}
            {:start (t/instant "2023-09-01T00:00:00Z")
             :end (t/instant "2023-10-01T00:00:00Z")})

(b/get-bars im
            {:asset "MSFT"
             :calendar [:us :d]
             :import :kibot}
            {:start (t/instant "2019-12-01T00:00:00Z")
             :end (t/instant "2020-02-01T00:00:00Z")})

; eodhd

(b/get-bars im
            {:asset "AEE.AU"
             :calendar [:us :d]
             :import :eodhd}
            {:start (-> "2023-12-01T00:00:00Z" t/instant)
             :end (-> "2024-04-01T00:00:00Z" t/instant)})

(b/get-bars im
            {:asset "AEE.AU"
             :calendar [:us :d]
             :import :eodhd}
            ; fails because more than a year ago
            {:start (-> "2020-12-01T00:00:00Z" t/instant)
             :end (-> "2024-04-01T00:00:00Z" t/instant)})
