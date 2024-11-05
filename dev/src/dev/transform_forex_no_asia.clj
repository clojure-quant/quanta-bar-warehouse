(ns dev.transform-forex-no-asia
  (:require
   [clojure.pprint :refer [print-table]]
   [tick.core :as t]
   [tablecloth.api :as tc]
   [missionary.core :as m]
   [ta.db.bars.protocol :as b]
   [dev.env :refer [bar-engine]]
   [quanta.bar.preload :refer [import-bars]]
   [quanta.bar.transform.forex-no-asia :refer [parse-interval-kw]]
   [quanta.calendar.core :as c]))

;;
;; Import data
;;
(m/? (import-bars
      bar-engine
      {:asset "EUR/USD"
       :calendar [:forex :m]
       :import :kibot-http
       :to :nippy
       :window {:start (t/instant "2024-11-04T12:00:00Z")
                :end (t/instant "2024-11-04T18:00:00Z")}
       :label "import kibot :m"}))

; data available:
(m/?
 (b/get-bars bar-engine
             {:asset "EUR/USD"
              :calendar [:forex :m]
              :bardb :nippy}
             {}))

;; transform
(m/?
 (b/get-bars bar-engine
             {:asset "EUR/USD"
              :calendar [:forex :m5]
              :bardb :nippy
              :to :nippy
              :import :kibot-http
              :transform :forex-no-asia}
             {:start (t/instant "2024-10-01T12:00:00Z")
              :end (t/instant "2024-11-04T18:00:00Z")}))

; data available:
(m/?
 (b/get-bars bar-engine
             {:asset "EUR/USD"
              :calendar [:forex-no-asia :m5]
              :bardb :nippy}
             {}))

;;
(parse-interval-kw :m4)