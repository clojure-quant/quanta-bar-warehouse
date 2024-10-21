(ns dev.preload
  (:require
   [missionary.core :as m]
   [tick.core :as t]
   [ta.db.bars.protocol :as b :refer [barsource bardb]]
   [quanta.bar.preload :refer [import-bars import-bars-one]]
   [dev.env :refer [bar-engine]]))

;; test the import-one routine 
;; this should not be used by users

(m/? (import-bars-one
      bar-engine
      {:asset "EUR/USD" :calendar [:us :d]
       :import :kibot-http
       :to :nippy
       :window {:start (t/instant "2000-01-01T00:00:00Z")
                :end (t/instant "2024-09-10T00:00:00Z")}
       :label "test"}))
;; => {:asset "EUR/USD",
;;     :window-count 6441,
;;     :start #time/zoned-date-time "1997-01-14T16:30-05:00[America/New_York]",
;;     :count 7099,
;;     :end #time/zoned-date-time "2024-10-18T16:30-04:00[America/New_York]"}

;; bybit

(m/?
 (import-bars
  bar-engine
  {:asset ["BTCUSDT" "ETHUSDT"]
   :parallel-nr 100
   :calendar [:crypto :d]
   :import :bybit-parallel
   :to :nippy
   :window {:start (t/instant "2021-07-05T00:00:00Z")
            :end (t/instant "2024-09-10T00:00:00Z")}
   :label "test"}))

;; => ({:asset "BTCUSDT",
;;      :window-count 1162,
;;      :start #time/instant "2021-07-05T23:59:59.999999Z",
;;      :count 1162,
;;      :end #time/instant "2024-09-08T23:59:59.999999Z"}
;;     {:asset "ETHUSDT",
;;      :window-count 1162,
;;      :start #time/instant "2021-07-05T23:59:59.999999Z",
;;      :count 1162,
;;      :end #time/instant "2024-09-08T23:59:59.999999Z"})

(m/?
 (import-bars
  bar-engine
  {:asset ["BTCUSDT" "ETHUSDT"]
   :parallel-nr 100
   :calendar [:crypto :h]
   :import :bybit-parallel
   :to :nippy
   :window {:start (t/instant "2024-01-01T00:00:00Z")
            :end (t/instant "2024-09-10T00:00:00Z")}
   :label "test"}))

;; kibot
;; note that assets have to be in asset-db

(m/?
 (import-bars
  bar-engine
  {:asset ["EUR/USD" "USD/JPY"] :calendar [:forex :d]
   :parallel-nr 100
   :import :kibot-http
   :to :nippy
   :window {:start (t/instant "2000-01-01T00:00:00Z")
            :end (t/instant "2024-09-10T00:00:00Z")}
   :label "multiple forex quotes test"}))
;; => ({:asset "EUR/USD",
;;      :window-count 6441,
;;      :start #time/zoned-date-time "1997-01-14T16:30-05:00[America/New_York]",
;;      :count 7099,
;;      :end #time/zoned-date-time "2024-10-18T16:30-04:00[America/New_York]"}
;;     {:asset "USD/JPY",
;;      :window-count 6441,
;;      :start #time/zoned-date-time "1997-01-17T16:30-05:00[America/New_York]",
;;      :count 7099,
;;      :end #time/zoned-date-time "2024-10-18T16:30-04:00[America/New_York]"})

(m/?
 (import-bars
  bar-engine
  {:asset ["EUR/USD" "USD/JPY"]
   :calendar [:forex :m]
   :parallel-nr 100
   :import :kibot-http
   :to :nippy
   :window {:start (t/instant "2023-05-22T00:00:00Z")
            :end (t/instant "2024-10-21T00:00:00Z")}
   :label "multiple forex quotes test"}))
;; => ({:asset "EUR/USD",
;;      :window-count 521222,
;;      :start #time/zoned-date-time "2023-05-22T00:00-04:00[America/New_York]",
;;      :count 527627,
;;      :end #time/zoned-date-time "2024-10-21T12:11-04:00[America/New_York]"}
;;     {:asset "USD/JPY",
;;      :window-count 521222,
;;      :start #time/zoned-date-time "2023-05-22T00:00-04:00[America/New_York]",
;;      :count 526051,
;;      :end #time/zoned-date-time "2024-10-21T12:14-04:00[America/New_York]"})

(m/? (import-bars
      bar-engine
      {:asset ["EUR/USD" "USD/JPY"]
       :calendar [:forex :h]
       :bardb :nippy
       :transform :compress
       :to :nippy
       :window {:start (t/instant "2023-05-22T00:00:00Z")
                :end (t/instant "2024-10-21T00:00:00Z")}
       :label "bar compression"}))

(m/?
 (b/get-bars bar-engine
             {:asset "EUR/USD"
              :calendar [:forex :d]
              :bardb :nippy}
             {:start (t/instant "2023-05-22T00:00:00Z")
              :end (t/instant "2024-10-21T00:00:00Z")}))



