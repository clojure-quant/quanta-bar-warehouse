(ns quanta.bar.generator
  (:require
   [tick.core :as t]
   [missionary.core :as m]
   [taoensso.timbre :as timbre :refer [debug info warn error]]
   [tablecloth.api :as tc]
   [quanta.calendar.interval :as i]
   [quanta.calendar.env.scheduler :refer [get-calendar-flow]]
   [ta.db.bars.protocol :refer [append-bars]]
   [quanta.bar.generator.flow :refer [bar-f]]
   [quanta.bar.generator.util :refer [log-flow-to]]))

#_(defn extended-quote-f [market-kw quote-f]
    (m/eduction
     (map (fn [quote] (assoc quote
                             :market market-kw
                             :timestamp (t/instant))))
     quote-f))

(def state (atom {}))

(defn start-generating-clock [trade-f clock-t market-kw]
  (let [;equote-f (extended-quote-f market-kw trade-f)
        ;quote-block-f (m/eduction (partition-all 100) equote-f)
        generated-bar-f (bar-f clock-t trade-f)]
    (swap! state assoc market-kw
           {:bar-writer (log-flow-to "bars.log" generated-bar-f)})))

(defn start-generating [{:keys [db]} trade-f calendar]
  (let [[market-kw interval-kw] calendar
        clock-t (m/rdv)
        calendar-f (get-calendar-flow calendar)
        dt-f (m/eduction (map i/current)
                         (map :close)
                         calendar-f)
        calendar-done-f  (m/ap (let [dt (m/?> dt-f)
                                     dt-inst (t/instant dt)]
                                 (info "generate-bars " calendar dt-inst " start..")
                                 (m/? (clock-t dt-inst)) ; trigger generation and wait until finished
                                 (info "generate-bars " calendar dt-inst " success!")
                                 dt-inst))
        generated-bar-f (bar-f clock-t trade-f)
        written-bar-f (m/ap (let [bar-ds (m/?> generated-bar-f)]
                              ;(println "generated bar-ds: " bar-ds)
                              (info "bar-generator has written bars #: " (tc/row-count bar-ds))
                              (m/? (append-bars db {:calendar calendar} bar-ds))))
        bar-writer-t (m/reduce (fn [_s _v] nil) nil written-bar-f)
        calendar-consumer-t (m/reduce (fn [s v] nil) nil calendar-done-f)
        runner-t (m/join vector bar-writer-t calendar-consumer-t)]
    (swap! state assoc market-kw
           {:runner (runner-t
                     (fn [_] (info "bar-generator-task completed\n"))
                     (fn [ex] (error "bar-generator-task crash\n " ex)))
            :calendar-f calendar-done-f})))

(defn stop-generating [market-kw]
  (when-let [{:keys [runner]} (market-kw @state)]
    (swap! state dissoc market-kw)
    (runner)))