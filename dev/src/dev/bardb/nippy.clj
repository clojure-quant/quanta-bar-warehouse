(ns dev.bardb.nippy
  (:require
   [tick.core :as t]
   [missionary.core :as m]
   [tablecloth.api :as tc]
   [quanta.bar.protocol :as b]
   [quanta.bar.db.nippy.overview :refer [create-summary]]
   [quanta.bar.db.nippy :refer [summary-nippy]]
   [dev.env :refer [bardbnippy]]))

(def qqq (m/? (b/get-bars bardbnippy {:asset "QQQ" :calendar [:us :d]} {})))
(def spy (m/? (b/get-bars bardbnippy {:asset "SPY" :calendar [:us :d]} {})))
qqq
spy
(def consolidated-ds (tc/concat qqq spy))

consolidated-ds

(create-summary consolidated-ds)

(summary-nippy bardbnippy [:us :d])

