(ns dev.duck.demo1
  (:require
   [tick.core :as t]
   [missionary.core :as m]
   [tablecloth.api :as tc]
   [ta.db.bars.protocol :as b]
   [quanta.bar.db.duck :as duck]))

(def db (duck/start-bardb-duck "duck11.ddb"))

db

(def bar-ds
  (-> {:date [(t/instant "2022-03-05T00:00:00Z")
              (t/instant "2023-03-06T20:00:00Z")
              (t/instant "2024-03-06T20:00:00Z")]
       :open [10.0 20.0 30.0]
       :high [10.0 20.0 30.0]
       :low [10.0 20.0 30.0]
       :close [10.0 20.0 30.0]
       :volume [10.0 20.0 30.0]
       :asset "XXX"}
      tc/dataset))

bar-ds

(m/? (b/append-bars db {:asset "XXX"
                        :calendar [:us :d]}
                    bar-ds))

(m/? (b/get-bars db {:asset "XXX"
                     :calendar [:us :d]}
                 {:start (t/instant "2022-03-05T00:00:00Z")
                  :end (t/instant "2025-03-06T20:00:00Z")}))

(m/? (b/get-bars db {:asset "XXX"
                     :calendar [:us :d]}
            ; make request in zoned dt, to test if auto-instant convert works
                 {:start (t/zoned-date-time "2021-03-07T16:30-05:00[America/New_York]")
                  :end (t/zoned-date-time "2026-05-08T16:30-04:00[America/New_York]")}))

; unknown dataset
(m/? (b/get-bars db {:asset "EUR/USD"
                     :calendar [:forex :d]}
                 {:start (t/instant "2022-03-05T00:00:00Z")
                  :end (t/instant "2025-03-06T20:00:00Z")}))

(require '[tech.v3.dataset :as ds])
(def stocks
  (ds/->dataset "https://github.com/techascent/tech.ml.dataset/raw/master/test/data/stocks.csv"
                {:key-fn keyword
                 :dataset-name :stocks}))
(tc/info stocks)