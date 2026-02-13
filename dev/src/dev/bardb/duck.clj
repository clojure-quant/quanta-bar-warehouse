(ns dev.bardb.duck
  (:require
   [tick.core :as t]
   [missionary.core :as m]
   [tech.v3.dataset :as tds]
   [tablecloth.api :as tc]
   [quanta.calendar.window :as w]
   [quanta.bar.protocol :as b]
   [quanta.bar.db.duck :as duck]
   [quanta.bar.db.duck.warehouse :as wh]
   [dev.env :refer [bardbduck]]))

(def stocks
  (tds/->dataset "https://github.com/techascent/tech.ml.dataset/raw/master/test/data/stocks.csv"
                 {:key-fn keyword
                  :dataset-name :stocks}))
(tc/info stocks)

;; this format needs to be patched, so we can save it 
; symbol  -> asset
; date - date needs to become an instant at midnight
; price  -> open/high/low/close and zero volume

(defn add-datetime [d]
  (-> d
      (t/date)
      (t/at  (t/midnight)) ; LocalDateTime at 00:00
      (t/in "UTC")         ; ZonedDateTime in UTC
      t/instant))          ; Instant

(def stocks-import
  (-> stocks
      (tc/rename-columns {:symbol :asset})
      (tc/add-columns
       {:date (map add-datetime (:date stocks))
        :open (:price stocks)
        :high (:price stocks)
        :low (:price stocks)
        :close (:price stocks)
        :volume 0.0})))

(m/? (b/append-bars bardbduck {:asset "XXX"
                               :calendar [:us :d]}
                    stocks-import))

(wh/warehouse-summary bardbduck [:us :d])

(m/? (b/get-bars bardbduck
                 {:asset "GOOG"}
                 (w/date-range->window [:us :d]
                                       {:start (t/instant "2005-01-01T00:00:00Z")
                                        :end (t/instant "2010-03-01T20:00:00Z")})))

(m/? (b/get-bars bardbduck
                 {:asset "GOOG"
                  :calendar [:us :d]}
                 {:start (t/instant "2005-01-01T00:00:00Z")
                  :end (t/instant "2010-03-01T20:00:00Z")}))

(m/? (b/get-bars bardbduck
                 {:asset "GOOG"
                  :calendar [:us :d]}
                 {; entire series
                  }))

(m/? (b/get-bars bardbduck
                 {:asset "GOOG"
                  :calendar [:us :d]}
                 {:start (t/instant "2007-01-01T00:00:00Z")
                  ; starting in 2007
                  }))

(m/? (b/get-bars bardbduck
                 {:asset "GOOG"
                  :calendar [:us :d]}
                 {:end (t/instant "2009-01-01T00:00:00Z")
                  ; ending in 2009
                  }))

(m/? (b/get-bars bardbduck
                 {:asset "GOOG"
                  :calendar [:us :d]}
                 {:end (t/instant "2009-01-01T00:00:00Z")
                  :n 10
                  ; ending in 2009
                  }))

; test for unknown asset
(b/get-bars bardbduck
            {:asset "AEE.AU"
             :calendar [:us :d]
             :import :kibot}
            window)

(wh/warehouse-summary bardbduck [:us :d])

(m/? (b/delete-bars bardbduck {:asset "AMZN"
                               :calendar [:us :d]}))

(wh/warehouse-summary bardbduck [:us :d])


