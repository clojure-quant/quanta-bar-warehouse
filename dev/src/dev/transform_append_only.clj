(ns dev.transform-append-only
  (:require
    [tick.core :as t]
    [missionary.core :as m]
    [ta.db.bars.protocol :as b]
    [dev.env :refer [bar-engine]]
    ;[quanta.bar.preload :refer [import-bars import-bars-one]]
    [quanta.bar.transform.append-only :refer [missing-bars-windows]]
    ))

;; import
;; import bybit :m
;(m/? (import-bars
;       bar-engine
;       {:asset "BTCUSDT"
;        :calendar [:crypto :m]
;        ;:bardb :nippy
;        ;:transform :compress
;        :import :bybit-parallel
;        :to :nippy
;        :window {:start (t/instant "2024-09-01T00:00:00Z")
;                 :end (t/instant "2024-09-09T00:00:00Z")}
;        :label "import bybit :m"}))



; data available:
;| BTCUSDT | 2021-07-05T23:59:59.999999Z | 2024-09-08T23:59:59.999999Z |   1162 |          1162 |

; full month available
(m/?
  (b/get-bars bar-engine
              {:asset "BTCUSDT"
               :calendar [:crypto :d]
               :bardb :nippy}
              {:start (t/instant "2024-08-01T00:00:00Z")
               :end (t/instant "2024-09-01T00:00:00Z")}))

; month with missing data
(m/?
  (b/get-bars bar-engine
              {:asset "BTCUSDT"
               :calendar [:crypto :d]
               :bardb :nippy}
              {:start (t/instant "2024-09-01T00:00:00Z")
               :end (t/instant "2024-10-01T00:00:00Z")}))

(m/?
  (b/get-bars bar-engine
              {:asset "BTCUSDT"
               :calendar [:crypto :m]
               :bardb :nippy}
              {:start (t/instant "2024-09-01T00:00:00Z")
               :end (t/instant "2024-10-01T00:00:00Z")}))

; compress
(m/?
  (b/get-bars bar-engine
              {:asset "BTCUSDT"
               :calendar [:crypto :d]
               :bardb :nippy
               :transform :append-only}
              {:start (t/instant "2024-09-01T00:00:00Z")
               :end (t/instant "2024-10-01T00:00:00Z")}))


; TODO: warum liefert transform anderes window als get-bars :m oder :d??? -1h ...

(m/?
  (b/get-bars bar-engine
              {:asset "BTCUSDT"
               :calendar [:crypto :h]
               :bardb :nippy
               :to :nippy
               :transform :compress}
              {:start (t/instant "2024-09-01T00:00:00Z")
               :end (t/instant "2024-10-01T00:00:00Z")}))

(m/?
  (b/get-bars bar-engine
              {:asset "BTCUSDT"
               :calendar [:crypto :m]
               :bardb :nippy
               :to :nippy
               :transform :compress}
              {:start (t/instant "2024-09-01T00:00:00Z")
               :end (t/instant "2024-10-01T00:00:00Z")}))


;; append-only
(m/?
  (b/get-bars bar-engine
              {:asset "BTCUSDT"
               :calendar [:crypto :d]
               :bardb :nippy
               :to :nippy
               :transform :append-only}
              {:start (t/instant "2024-09-01T00:00:00Z")
               :end (t/instant "2024-10-01T00:00:00Z")}))

(let [cal [:crypto :d]
      ds (m/?
           (b/get-bars bar-engine
                       {:asset "BTCUSDT"
                        :calendar  [:crypto :d]
                        :bardb :nippy}
                       {:start (t/instant "2024-09-01T00:00:00Z")
                        :end (t/instant "2024-09-10T00:00:00Z")}))]
  (missing-bars-windows ds
                        cal
                        {:start (t/instant "2024-09-01T00:00:00Z")
                         :end (t/instant "2024-09-05T00:00:00Z")}))

