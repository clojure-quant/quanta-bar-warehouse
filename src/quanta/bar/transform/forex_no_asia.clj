(ns quanta.bar.transform.forex-no-asia
  (:require
    [taoensso.timbre :as timbre :refer [debug info warn error]]
    [missionary.core :as m]
    [tablecloth.api :as tc]
    [tick.core :as t]
    [ta.calendar.validate :as cal]
    [ta.db.bars.protocol :refer [barsource] :as b]
    [ta.calendar.calendars :refer [get-calendar] :as calendars]
    [quanta.bar.transform.helper :refer [write-bars]])
  (:import (java.io FileNotFoundException)))

(defn parse-interval-kw [interval-kw]
  (let [re #"([mh]+)(\d+)?"
        match (re-matches re (name interval-kw))]
    (when match
      {:unit (case (nth match 1)
               "m" :minutes
               "h" :hours)
       :n (if (nth match 2)
            (Integer/parseInt (nth match 2))
            1)})))

(defn append-bars [opts bar-ds]
  (assert (contains? opts :to) ":to not set. need target bardb for appending")
  (let [{:keys [engine to asset calendar]} opts
        opts-clean {:asset asset
                    :calendar calendar
                    :bardb to}]
    (info "filter calendar (forex-no-asia) - append-bars" opts-clean)
    (b/append-bars engine opts-clean bar-ds)))


;NOTE: expects :forex calendar because the day-open? check is left out for performance reasons
(defrecord transform-forex-no-asia []
  barsource
  (get-bars [this opts window]
    (m/sp
     (info "get-bars" (select-keys opts [:task-id :asset :calendar :import])
           "window:" (select-keys window [:start :end]))
     (try
       (let [{:keys [calendar engine]} opts
             to-calendar-kw :forex-no-asia               ; hardcoded
             to-interval-kw (cal/interval calendar)
             _ (info "filter calendar (forex-no-asia) - " calendar "-> [" to-calendar-kw to-interval-kw "]")
             _ (assert (cal/validate-calendar calendar))
             _ (assert (cal/validate-calendar [to-calendar-kw to-interval-kw]))
             {:keys [open close timezone] :as to-calendar-spec} (get-calendar to-calendar-kw)
             {:keys [n unit] :as interval-map} (parse-interval-kw to-interval-kw)
             _ (assert interval-map (str "interval-kw could not be parsed or to big: " interval-map))
             first-close (t/>> open (t/new-duration n unit))
             ; source calendar
             bar-ds (try
                      (debug "filter calendar (forex-no-asia) - forwarding to append-only transform")
                      (m/? (b/get-bars engine
                                       (assoc opts :transform :append-only)
                                       window))
                      (catch FileNotFoundException ex
                        (tc/dataset [])))
             ; target calendar
             filtered-ds (if (tc/empty-ds? bar-ds)
                           bar-ds
                           (tc/select-rows bar-ds
                                           #(let [zoned-dt (t/in (:date %) timezone)
                                                  time (t/time zoned-dt)]
                                              (and (t/>= time first-close)
                                                   (t/<= time close)))
                                           :date))
             ]
         (info "filter calendar (forex-no-asia) - result: " (tc/row-count filtered-ds) " of total bars:" (tc/row-count bar-ds))
         (write-bars (assoc opts :calendar [to-calendar-kw to-interval-kw])
                     filtered-ds))
       (catch AssertionError ex
         (error "filter calendar (forex-no-asia) - assertion:" window opts " exception: " ex))
       (catch Exception ex
         (error "filter calendar (forex-no-asia) - error" window opts " exception: " ex))))))

(defn start-transform-forex-no-asia []
  (transform-forex-no-asia.))
