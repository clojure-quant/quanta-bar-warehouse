(ns dev.env
  (:require
   [modular.env :refer [env]]
   [modular.log :refer [timbre-config!]]
   [quanta.bar.db.nippy :as nippy]
   [quanta.bar.db.duck :as duck]
   [quanta.bar.split.service :refer [start-split-service]]
   [quanta.bar.db.random :refer [start-random-bar-source]]))

(timbre-config!
 {:min-level [[#{"org.eclipse.jetty.*"} :warn]
              [#{"modular.oauth2.token.refresh"} :warn]
              [#{"*"} :info]]
  :appenders {:default {:type :console-color}}})

(def bardbnippy  (nippy/start-bardb-nippy (env "${QUANTASTORE}/bardb/eodhd-nippy/")))

(def bardbduck (duck/start-bardb-duck "stocks.ddb"))

(def ss (start-split-service {:bardb bardbduck}))

(def rs (start-random-bar-source {:seed 42
                                  :zero-price 100.0}))

rs
