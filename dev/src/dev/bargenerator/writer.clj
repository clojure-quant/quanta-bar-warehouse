(ns dev.bargenerator.writer
  (:require
   [missionary.core :as m]
   [ta.db.bars.protocol :as b]
   [quanta.bar.db.textlogger :refer [start-textlogger]]
   [quanta.bar.db.duck :as duck]
   [quanta.bar.generator :refer [start-generating stop-generating]]
   [dev.bargenerator.randomfeed :refer [trade-feed]]))

(def db (start-textlogger "bars.log"))

(start-generating
 {:db db}
 trade-feed
 [:crypto :m])

;; it will take up to a minute, before the first bars are showing up
;; in bars.log

(stop-generating [:crypto :m])

(def db-duck (duck/start-bardb-duck "random-quotes.ddb"))

(start-generating
 {:db db-duck}
 trade-feed
 [:crypto :m])

; get all data available for "A"
(m/? (b/get-bars db-duck
                 {:asset "A"
                  :calendar [:crypto :m]}
            ; unlimited window
                 {}))

(m/? (b/get-bars db-duck
                 {:asset "B"
                  :calendar [:crypto :m]}
            ; unlimited window
                 {}))
