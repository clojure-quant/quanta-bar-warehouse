(ns dev.bardb.bardb-load
  (:require
   [tick.core :as t]
   [tablecloth.api :as tc]
   [quanta.bar.protocol :as b]
   [quanta.bar.db.nippy :as nippy]
   [modular.system]))

(def db (modular.system/system :duckdb))

db

;(def db (nippy/start-bardb-nippy "output/kibot-intraday/"))

(def window {:start (t/instant "2022-03-05T00:00:00Z")
             :end (t/instant "2024-03-20T20:00:00Z")})

(b/get-bars db
            {:asset "JY0"
             :calendar [:forex :m]}
            window)

(b/get-bars db
            {:asset "EUR/USD"
             :calendar [:forex :m]}
            window)

(b/get-bars db
            {:asset "USD/JPY"
             :calendar [:forex :d]}
            window)

(-> (b/get-bars db
                {:asset "QQQ"  ; "USD/JPY"
                 :calendar [:forex :m]}
                window))

;; 2024-03-15T14:17-04:00[America/New_York] etf 30 MIN DELAYED.
;; 2024-03-15T12:14-04:00[America/New_York]  forex (seems to be UK close)
;; 2024-03-15T03:22-04:00[America/New_York]  future (seems to be tokyo close)

(b/get-bars db
            {:asset "ETHUSDT"  ; "USD/JPY"
             :calendar [:crypto :m15]}
            {:start #inst "2024-07-01T01:00:00.000-00:00"
             :end #inst "2024-07-05T16:45:00.000000000-00:00"})

(b/get-bars db
            {:asset "ETHUSDT"  ; "USD/JPY"
             :calendar [:crypto :m15]}
            {})

(b/get-bars db
            {:asset "ETHUSDT"  ; "USD/JPY"
             :calendar [:crypto :m15]}
            {:start #inst "2024-07-01T01:00:00.000-00:00"})

(b/get-bars db
            {:asset "ETHUSDT"  ; "USD/JPY"
             :calendar [:crypto :m15]}
            {:start (t/instant "2024-07-01T01:00:00.000-00:00")})


