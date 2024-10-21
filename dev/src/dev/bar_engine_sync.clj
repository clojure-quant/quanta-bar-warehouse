
(ns dev.bar-engine-sync
  (:require
   [tick.core :as t]
   [missionary.core :as m]
   [ta.db.bars.protocol :as b]
   [quanta.bar.engine :refer [create-engine-sync]]
   [dev.env :refer [bar-engine]]))

(def dt (t/instant "2024-05-01T00:00:00Z"))
dt

(def se  (create-engine-sync bar-engine))

(b/get-bars
 se {:asset "ETHUSDT" ; crypto
     :calendar [:crypto :h]
     :bardb :nippy}
 {:start  (t/instant "2024-05-01T00:00:00Z")
  :end (t/instant "2024-07-01T00:00:00Z")})


