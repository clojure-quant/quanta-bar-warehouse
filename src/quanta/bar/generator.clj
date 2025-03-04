(ns quanta.bar.generator
  (:require
   [tick.core :as t]
   [missionary.core :as m]
   [quanta.calendar.scheduler :refer [get-calendar-flow]]
   [quanta.bar.generator.flow :refer [bar-f]]
   [quanta.bar.generator.util :refer [log-flow-to]]))

(defn extended-quote-f [market-kw quote-f]
  (m/eduction
   (map (fn [quote] (assoc quote
                           :market market-kw
                           :timestamp (t/instant))))
   quote-f))

(def state (atom {}))

(defn start-generating-clock [trade-f clock-t market-kw]
  (let [equote-f (extended-quote-f market-kw trade-f)
        quote-block-f (m/eduction (partition-all 100) equote-f)
        generated-bar-f (bar-f clock-t trade-f)]
    (swap! state assoc market-kw
           {:bar-writer (log-flow-to "quotes.log" quote-block-f)
            :quote-writer (log-flow-to "bars.log" generated-bar-f)})))

(defn start-generating [trade-f calendar]
  (let [[market-kw interval-kw] calendar
        equote-f (extended-quote-f market-kw trade-f)
        quote-block-f (m/eduction (partition-all 100) equote-f)
        clock-t (m/rdv)
        calendar-f (get-calendar-flow calendar)
        calendar-done-f  (m/ap (let [dt (m/?> calendar-f)
                                     dt-inst (t/instant dt)]
                                 (println "dt: " dt-inst)
                                 (m/? (clock-t dt-inst)) ; trigger generation and wait until finished
                                 (println "bar-generator has written bars for " dt calendar)
                                 dt-inst))
        generated-bar-f (bar-f clock-t trade-f)
        runner-t (m/reduce (fn [s v] nil) nil calendar-done-f)]
    (swap! state assoc market-kw
           {:bar-writer (log-flow-to "quotes.log" quote-block-f)
            :quote-writer (log-flow-to "bars.log" generated-bar-f)
            :runner (runner-t
                     (fn [_] (println "\nbar-generator-task completed\n"))
                     (fn [ex] (println "\nbar-generator-task crash\n " ex)))
            :calendar-f calendar-done-f})))

(defn stop-generating [market-kw]
  (when-let [{:keys [bar-writer quote-writer]} (market-kw @state)]
    (swap! state dissoc market-kw)
    (bar-writer)
    (quote-writer)))

@state

(def x (m/rdv))
(m/? (x 3))