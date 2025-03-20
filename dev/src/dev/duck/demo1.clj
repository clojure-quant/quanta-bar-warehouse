(ns dev.duck.demo1
  (:require
   [tick.core :as t]
   [missionary.core :as m]
   [tablecloth.api :as tc]
   [ta.db.bars.protocol :as b]
   [quanta.calendar.window :as w]
   [quanta.bar.db.duck :as duck]
   [quanta.bar.db.duck.warehouse :as wh]
   [quanta.bar.compressor :refer [compress]]
   [quanta.bar.db.duck.delete :as d]))

(def db (duck/start-bardb-duck "duck11.ddb"))

(def db (duck/start-bardb-duck "random-quotes.ddb"))

db

(duck/stop-bardb-duck db)

(wh/get-data-range db [:us :d] "XXX")

(wh/warehouse-summary db [:crypto :m])
(wh/warehouse-summary db [:crypto :h])

(d/delete-bars-asset-dt db [:us :d] "XXX" (t/instant "2022-03-05T00:00:00.000Z"))
(d/delete-bars-asset-dt db [:us :d] "XXX" (t/instant "2022-03-06T20:00:00Z"))
(t/instant)

(d/delete-calendar db [:crypto :h])

(m/?
 (compress db {:calendar-from [:crypto :m]
               :calendar-to [:crypto :h]}))

(m/? (b/get-bars db {:asset "A"}
                 (w/date-range->window [:crypto :m]
                                       {:start (t/instant "2025-03-04T00:00:00Z")
                                        :end (t/instant "2025-03-06T20:00:00Z")})))

(m/? (b/get-bars db {:asset "A"}
                 (w/date-range->window [:crypto :h]
                                       {:start (t/instant "2025-03-04T00:00:00Z")
                                        :end (t/instant "2025-03-06T20:00:00Z")})))

(def bar-ds
  (-> {:date [(t/instant "2022-03-05T00:00:00Z")
              (t/instant "2023-03-06T20:00:00Z")
              (t/instant "2024-03-06T20:00:00Z")
              (t/instant "2025-03-05T22:00:00Z")]
       :open [10.0 20.0 30.0 40.0]
       :high [10.0 20.0 30.0 40.0]
       :low [10.0 20.0 30.0 40.0]
       :close [10.0 20.0 30.0 40.0]
       :volume [10.0 20.0 30.0 40.0]
       :asset "XXX"}
      tc/dataset))

bar-ds

(m/? (b/append-bars db {:asset "XXX"
                        :calendar [:us :d]}
                    bar-ds))

(m/? (b/get-bars db {:asset "XXX"}
                 (w/date-range->window [:us :d]
                                       {:start (t/instant "2021-03-05T00:00:00Z")
                                        :end (t/instant "2025-03-06T20:00:00Z")})))

(->  (w/date-range->window [:us :d]
                           {:start (t/instant "2021-03-05T22:00:00Z")
                            :end (t/instant "2025-03-05T22:00:00Z")})
     (w/window->close-range))

(m/? (b/get-bars db {:asset "XXX"}
                 (w/date-range->window [:us :d]
                                       {:start (t/instant "2025-03-05T22:00:00Z")
                                        :end (t/instant "2025-03-05T22:00:00Z")})))

; unknown dataset
(m/? (b/get-bars db {:asset "EUR/USD"}
                 (w/date-range->window [:forex :d]
                                       {:start (t/instant "2021-03-05T00:00:00Z")
                                        :end (t/instant "2025-03-06T20:00:00Z")})))

(require '[tech.v3.dataset :as ds])
(def stocks
  (ds/->dataset "https://github.com/techascent/tech.ml.dataset/raw/master/test/data/stocks.csv"
                {:key-fn keyword
                 :dataset-name :stocks}))
(tc/info stocks)