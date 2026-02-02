(ns quanta.bar.compressor
  (:require
   [taoensso.timbre :as timbre :refer [info warn error]]
   [missionary.core :as m]
   [tick.core :as t]
   [tablecloth.api :as tc]
   [quanta.bar.protocol  :as b]
   [quanta.calendar.window :as w]
   [quanta.calendar.ds.compress :refer [compress-to-calendar]]
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
   (info "compressing from: " calendar-from " to: " calendar-to)
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
                                  (m/sp
                                   (try
                                     (let [date-range {:start start :end end}
                                           w (w/date-range->window calendar-from date-range)
                                           _ (info "date-range: " date-range
                                                   "window: " (w/window->close-range w))
                                           bar-ds (m/? (b/get-bars bar-db {:asset asset} w))
                                           bar-ds  (tc/select-rows bar-ds #(t/> (:date %) start))
                                           bars2 (compress-to-calendar bar-ds calendar-to)
                                           ; bars2 have :count column. If this is not 60 (for min->hour), 
                                           ; we could filter them, or write a warning.
                                           ]
                                       (m/? (b/append-bars bar-db {:asset asset
                                                                   :calendar calendar-to} bars2))
                                       bars2)
                                     (catch Exception ex
                                       (error "compress-bars " asset start end " failed: " (ex-message ex))
                                       nil))))]
     (if (seq tasks)
       (m/? (apply m/join vector (map load-compress-append-t tasks)))
       nil))))





