(ns quanta.studio.bars.preload
  (:require
   [clojure.pprint :refer [print-table]]
   [taoensso.timbre :refer [info warn error]]
   [de.otto.nom.core :as nom]
   [tablecloth.api :as tc]
   [modular.system]
   [ta.db.bars.protocol :as b]
   [ta.db.bars.duckdb.delete :refer [delete-bars]]
   [ta.import.helper.retries :refer [with-retries]]))

;; TODO: make delete a interface, so it works for nippy and duckdb.

(defn- import-bars-one
  "imports bars from a bar-source to a bar-db.
   uses bar-engine
   same syntax as bar-engine, but additional:
   :to        one of :nippy :duckdb
   :window    {:from :to} both instant or zoneddatetime
   :labelS    optional label for logging
   :retries   optional, number of retries to import bars."
  [bar-engine
   {:keys [asset calendar
           to window
           retries
           label]
    :or {label ""
         retries 5}
    :as opts}]
  (info "getting " label " for: " asset)
  (let [opts-import (dissoc opts :to :window :label :retries)
        ; ds (b/get-bars bar-engine opts window)
        ds (with-retries retries b/get-bars bar-engine opts-import window)]
    (if (nom/anomaly? ds)
      (do
        (error "could not get asset: " asset)
        {:asset asset :count 0})
      (let [c (tc/row-count ds)
            opts-save {:asset asset
                       :calendar calendar
                       :bardb to}]
        (info "saving asset:" asset " count: " c " calendar: " calendar)
             ;(delete-bars to [:forex :d] asset)
        (b/append-bars bar-engine opts-save ds)
        {:asset asset
         :count c
         :start (-> ds tc/first :date first)
         :end (-> ds tc/last :date first)}))))

(defn import-bars-impl
  "imports bars from a bar-source to a bar-db.
   asset: either a single asset EUR/USD or multiple assets [EUR/USD USD/JPY]
   calendar: [:us :d]
   from one of :kibot :bybit :bybit-parallel
   to: a bar-db like :nippy :duckdb
   window {:from :to} both instant or zoneddatetime
   label: optional label for logging
   retries: optional, number of retries to import bars.
   this function is intended to be used to import data prior to doing backtests.
   the optional environment key settings are useful if you use a non-standard
   modular config
   intended for usecase in notebook / repl"
  [bar-engine
   {:keys [asset label]
    :as opts}]
  (if (or (seq? asset) (vector? asset))
    (let [result (map #(import-bars-one bar-engine (assoc opts :asset %)) asset)]
      (println "result for: " label)
      (print-table result)
      result)
    (import-bars-one bar-engine opts)))

(defn import-bars
  "imports bars from a bar-source to a bar-db.
   asset: either a single asset EUR/USD or multiple assets [EUR/USD USD/JPY]
   calendar: [:us :d]
   from one of :kibot :bybit :bybit-parallel
   to: a bar-db like :nippy :duckdb
   window {:from :to} both instant or zoneddatetime
   label: optional label for logging
   retries: optional, number of retries to import bars.
   this function is intended to be used to import data prior to doing backtests.
   the optional environment key settings are useful if you use a non-standard
   modular config
   intended for usecase in notebook / repl"
  [opts]
  (let [bar-engine (modular.system/system :bar-engine)]
    (import-bars-impl bar-engine opts)))

(comment

  (require '[tick.core :as t])
  (import-bars-one {} {:asset "EUR/USD" :calendar [:us :d]
                       :from :kibot :to :nippy
                       :window {:start (t/instant "2000-01-01T00:00:00Z")
                                :end (t/instant "2024-09-10T00:00:00Z")}
                       :label "test"})

  (import-bars {:asset "EUR/USD" :calendar [:us :d]
                :from :kibot :to :nippy
                :window {:start (t/instant "2000-01-01T00:00:00Z")
                         :end (t/instant "2024-09-10T00:00:00Z")}
                :label "test"})

  (import-bars {:asset ["EUR/USD" "USD/JPY"] :calendar [:us :d]
                :from :kibot :to :nippy
                :window {:start (t/instant "2000-01-01T00:00:00Z")
                         :end (t/instant "2024-09-10T00:00:00Z")}
                :label "multiple forex quotes test"})

; 
  )