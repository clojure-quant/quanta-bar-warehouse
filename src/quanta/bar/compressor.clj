(ns quanta.bar.compressor
  (:require
   [missionary.core :as m]
   [tick.core :as t]
   [tablecloth.api :as tc]
   [quanta.calendar.compress :refer [compress-to-calendar]]
   [ta.db.bars.protocol  :as b]
   [quanta.bar.db :refer [summary]]))

(defn table-dict [ds]
  (if ds
    (->> (tc/rows ds :as-maps)
         (map (juxt :asset identity))
         (into {}))
    {}))

(defn make-compress-task [from to]
  (if to
    (let [task {:asset (:asset from)
                :start (:end to)
                :end (:end from)}]
      (if (t/> (:end task) (:start task))
        task
        nil ; no compression needed
        ))
; if no dest bars, do entire range
    from))

(defn compress-tasks [bar-db {:keys [calendar-from
                                     calendar-to]}]
  (m/sp
   (let [summary-from (m/? (summary bar-db {:calendar calendar-from}))
         summary-to (m/? (summary bar-db {:calendar calendar-to}))
         dict-from (table-dict summary-from)
         dict-to (table-dict summary-to)]
     (->> dict-from
          (map (fn [[asset from]]
                 (let [to (get dict-to asset)]
                   (make-compress-task from to))))
          (remove nil?)))))

(defn compress [bar-db {:keys [calendar-from
                               calendar-to] :as convert}]
  (m/sp
   (let [tasks (m/? (compress-tasks bar-db convert))
         load-compress-append-t (fn [{:keys [asset start end]}]
                                  (m/sp (let [bar-ds (m/? (b/get-bars bar-db
                                                                      {:asset asset
                                                                       :calendar calendar-from}
                                                                      {:start start
                                                                       :end end}))
                                              bars2 (compress-to-calendar bar-ds calendar-to)
                                           ; bars2 have :count column. If this is not 60 (for min->hour), 
                                           ; we could filter them, or write a warning.
                                              ]
                                          (m/? (b/append-bars bar-db {:asset asset
                                                                      :calendar calendar-to} bars2))
                                          bars2)))]
     (if (seq tasks)
       (m/? (apply m/join vector (map load-compress-append-t tasks)))
       nil))))





