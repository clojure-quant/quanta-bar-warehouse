(ns quanta.bar.db.duck.table
  (:require
   [taoensso.timbre :as timbre :refer [debug info warn error]]
   [tmducken.duckdb :as duckdb]
   [ta.calendar.calendars :refer [get-calendar-list]]
   [ta.calendar.interval :refer [intervals]]
   [quanta.bar.db.duck.calendar :refer [bar-category->table-name]]
   [quanta.bar.db.duck.ds :refer [empty-ds]]))

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

(defn create-table [session calendar]
  (let [ds (empty-ds calendar)
        table-name (bar-category->table-name calendar)]
    (debug "creating table: " table-name)
    (duckdb/create-table! (:conn session) ds)))

(defn init-tables! [session]
  (info "init duck-db tables")
  (doall (map (partial create-table session)
              (all-table-defs))))

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

