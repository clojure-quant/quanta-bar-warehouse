(ns quanta.notebook.bardb.multi
  (:require
   [tick.core :as t]
   [missionary.core :as m]
   [quanta.bar.protocol :as b]
   [quanta.bar.multi-asset :refer [get-bars-assets]]
   [modular.system :refer [system]]))

(def bardb (system :bar-db-ss))
bardb

(m/? (b/get-bars bardb
                 {:asset "GOOG"
                  :calendar [:us :d]}
                 {:start (t/instant "2025-01-01T00:00:00Z")
                  :end (t/instant "2025-04-01T20:00:00Z")}))

(m/? (get-bars-assets bardb {:asset ["GOOG" "AAPL" "QQQ"]
                             :calendar [:us :d]}
                      {:start (t/instant "2025-01-01T00:00:00Z")
                       :end (t/instant "2025-04-01T20:00:00Z")}))

