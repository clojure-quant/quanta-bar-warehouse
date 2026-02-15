(ns quanta.bar.db.duck.impl.table
  (:require
   [taoensso.timbre :as timbre :refer [debug info warn error]]
   [tmducken.duckdb :as duckdb]
   [quanta.calendar.db.calendars :refer [get-calendar-list]]
   [quanta.calendar.db.interval :refer [all-intervals]]
   [quanta.bar.db.duck.impl.calendar :refer [bar-category->table-name]]
   [quanta.bar.db.duck.impl.ds :refer [empty-ds]]))

(defn make-table-defs [cals intervals]
  (let [make-one-cal (fn [c i]
                       [c i])]
    (->>
     (for [c cals
           i intervals]
       (make-one-cal c i))
     (into []))))

(defn- get-intervals []
  all-intervals
  ;(-> intervals keys)
  )

(defn all-table-defs []
  (let [cals (get-calendar-list)
        intervals (get-intervals)]
    (make-table-defs cals intervals)))

(defn create-table [conn calendar]
  (let [ds (empty-ds calendar)
        table-name (bar-category->table-name calendar)]
    (debug "creating table: " table-name)
    (duckdb/create-table! conn ds)))

(defn init-tables! [conn]
  (info "init duck-db tables")
  (doall (map (partial create-table conn)
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

