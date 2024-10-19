(ns quanta.bar.preload
  (:require
   [clojure.pprint :refer [print-table]]
   [taoensso.timbre :refer [info warn error]]
   [missionary.core :as m]
   [tablecloth.api :as tc]
   [ta.db.bars.protocol :as b]
   [quanta.calendar.core :refer [fixed-window]]
   [ta.db.bars.duckdb.delete :refer [delete-bars]]

   [ta.import.helper.retries :refer [with-retries]]))

;; TODO: make delete a interface, so it works for nippy and duckdb.

(defn import-bars-one
  "imports bars from a bar-source to a bar-db.
   uses bar-engine
   same syntax as bar-engine, but additional:
   :to        one of :nippy :duckdb
   :window    {:from :to} both instant or zoneddatetime
   :labelS    optional label for logging
   :retries   optional, number of retries to import bars."
  [bar-engine
   {:keys [asset calendar to window retries label]
    :or {label ""
         retries 5}
    :as opts}]
  (m/sp
   (info  label " importing bars for: " asset)
   (try
     (let [opts-import (dissoc opts :to :window :label :retries)
           ds (m/? (b/get-bars bar-engine opts-import window))
             ;(with-retries retries b/get-bars bar-engine opts-import window)
           c (tc/row-count ds)]
       (info "saving asset:" asset " count: " c " calendar: " calendar)
                  ;(delete-bars to [:forex :d] asset)
       (b/append-bars bar-engine {:asset asset
                                  :calendar calendar
                                  :bardb to} ds)
       {:asset asset
        :start (-> ds tc/first :date first)
        :end (-> ds tc/last :date first)
        :count c
        ;using fixed-window instead of fixed-window-open because importer should convert to bar close date time
        :window-count (-> (fixed-window calendar window) count)})
     (catch Exception ex
       (error "could not get bars for asset: " asset " error: " (ex-message ex))
       {:asset asset
        :count 0
        :error (ex-message ex)}))))

(defn- limit-task [sem blocking-task]
  (m/sp
   (m/holding sem (m/? blocking-task))))

(defn- run-tasks
  "runs multiple tasks"
  [tasks parallel-nr]
  ; from: https://github.com/leonoel/missionary/wiki/Rate-limiting#bounded-blocking-execution
  ; When using (via blk ,,,) It's important to remember that the blocking thread pool 
  ; is unbounded, which can potentially lead to out-of-memory exceptions. 
  ; A simple way to work around it is by using a semaphore to rate limit the execution:
  (let [sem (m/sem parallel-nr)
        tasks-limited (map #(limit-task sem %) tasks)
        summarize (fn [& args] args)]
    (apply m/join summarize tasks-limited)))

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
  [bar-engine
   {:keys [asset label parallel-nr]
    :or {parallel-nr 1}
    :as opts}]
  (if (or (seq? asset) (vector? asset))
    (m/sp
     (let [make-task #(import-bars-one bar-engine (assoc opts :asset %))
           tasks (map make-task asset)
           result (m/? (run-tasks tasks parallel-nr))]
       (println "result for: " label result)
       (print-table [:asset :start :end :count :window-count] result)
       result))
    (import-bars-one bar-engine opts)))

