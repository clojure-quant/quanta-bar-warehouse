(ns quanta.bar.generator.util
  (:require
   [tick.core :as t]
   [missionary.core :as m])
  (:import [java.io StringWriter]))

(defn log-text [filename s]
  (spit filename s :append true))

(defn log-data [filename quotes]
  (let [sw (StringWriter.)
        _ (.write sw "\n\n\n")
        _ (doall
           (for [quote quotes]
             (let [quote-str (str "\n" quote)]
               (.write sw quote-str))))
        sdata (.toString sw)]
    (log-text filename sdata)))

(defn log-flow-to [filename flow]
  (let [writer-t (m/reduce (fn [_ quotes]
                             (log-data filename quotes)
                             nil)
                           nil flow)
        dispose (writer-t
                 (fn [_] (log-text filename "\nflow-writer-task completed\n"))
                 (fn [_] (log-text filename "\nflow-writer-task crash\n")))]
    dispose))

