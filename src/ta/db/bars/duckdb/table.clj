(ns ta.db.bars.duckdb.table
  (:require
   [tick.core :as t]
   [tablecloth.api :as tc]
   [tmducken.duckdb :as duckdb]
   [taoensso.timbre :as timbre :refer [debug info warn error]]
   [ta.calendar.calendars :refer [get-calendar-list]]
   [ta.calendar.interval :refer [intervals]]
   [ta.db.bars.duckdb.calendar :refer [bar-category->table-name]]))

(defn make-table-defs [cals intervals]
  (let [make-one-cal (fn [c i]
                       [c i])]
    (->>
     (for [c cals
           i intervals]
       (make-one-cal c i))
     (into []))))

(defn- get-intervals []
  (-> intervals keys))

(defn all-table-defs []
  (let [cals (get-calendar-list)
        intervals (get-intervals)]
    (make-table-defs cals intervals)))

(defn- now []
  (-> (t/now)
      ;(tick/date-time)
      (t/instant)))

(defn empty-ds [calendar]
  (let [table-name (bar-category->table-name calendar)]
    (-> (tc/dataset [{:open 0.0 :high 0.0 :low 0.0 :close 0.0
                      :volume 0.0 ; crypto volume is double.
                      :asset "000"
                      :date (now)
                      :epoch 0 :ticks 0}])
        (tc/set-dataset-name table-name))))

(defn create-table [session calendar]
  (let [ds (empty-ds calendar)
        table-name (bar-category->table-name calendar)]
    (debug "creating table: " table-name)
    (duckdb/create-table! (:conn session) ds)))

(defn init-tables [session]
  (let [exists? (:new? session)]
    (when (not exists?)
      (info "init duck-db tables")
      (doall (map (partial create-table session)
                  (all-table-defs))))))

(comment
  (get-intervals)
  (all-table-defs)
  (->   (all-table-defs)
        count)

  (make-table-defs [:us :forex :crypto]
                   [:m :h :d])

  (def fixed-table-defs
    [[:us :m]
     [:us :h]
     [:us :d]
     [:forex :d]
     [:forex :m]
     [:crypto :d]
     [:crypto :m]])

  (create-table db [:us :m])
  (create-table db [:us :h])

; 
  )

