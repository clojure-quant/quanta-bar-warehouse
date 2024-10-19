(ns quanta.bar.transform.dynamic
  (:require
   [taoensso.timbre :as timbre :refer [debug info warn error]]
   [ta.db.bars.protocol :refer [bardb barsource] :as b]
   [quanta.bar.transform.dynamic.overview-db :as overview]
   [quanta.bar.transform.dynamic.import :refer [import-on-demand]]))

(defrecord transform-dynamic [overview-db]
  barsource
  (get-bars [this opts window]
    (info "get-bars " (select-keys opts [:task-id :asset :calendar :import])
          " window: " (select-keys window [:start :end]))
    (if (:import opts)
      (let [opts-clean (dissoc opts :bardb)]
        (import-on-demand this opts-clean window))

      (debug "no import defined for: " opts))
    (b/get-bars (:bar-db this) opts window))
  bardb
  (append-bars [this opts ds-bars]
    (info "dynamic append-bars " opts ds-bars)
    ;(info "this: " this)
    (b/append-bars (:bar-db this) opts ds-bars)))

(defn start-bardb-dynamic [overview-path]
  (let [overview-db (overview/start-overview-db overview-path)]
    (transform-dynamic. overview-db)))

(defn stop-transform-dynamic [this]
  (overview/stop-overview-db (:overview-db this)))
