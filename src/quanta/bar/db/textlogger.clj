(ns quanta.bar.db.textlogger
  (:require
   [missionary.core :as m]
   [quanta.bar.protocol :refer [bardb]]))

(defn log-text [filename s]
  (spit filename s :append true))

(defrecord textlogger [filename]
  bardb
  (append-bars [_ opts bar-ds]
    (m/via m/blk
           (log-text filename (str "calendar: " (:calendar opts)))
           (log-text filename bar-ds))))

(defn start-textlogger [filename]
  (textlogger. filename))