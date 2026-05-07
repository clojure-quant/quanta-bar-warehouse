(ns quanta.bar.env
  (:require
   [taoensso.timbre :refer [trace info warn error]]
   [missionary.core :as m]
   [tablecloth.api :as tc]
   [quanta.calendar.window :as w]
   [quanta.calendar.interval :as i]
   [quanta.bar.protocol :as b]))

(defn get-bar-db [env]
  (let [bar-db (:bar-db env)]
    (assert bar-db ":env does not provide :bar-db")
    bar-db))

(defn- get-asset [spec]
  (:asset spec))

(defn- get-calendar [spec]
  (:calendar spec))

(defn get-bars
  "returns bars for asset/calendar/window"
  [env opts window]
  (let [asset (:asset opts)]
    (info "get-bars: " window)
    (assert asset "cannot get-bars for unknown asset!")
    (assert window "cannot get-bars for unknown window!")
    (b/get-bars (get-bar-db env) opts window)))

(defn get-trailing-bars [env {:keys [trailing-n] :as opts} last-interval]
  (info "get-trailing" trailing-n " bars ending: " (-> last-interval i/current :close))
  (m/sp
   (let [calendar (get-cal endar opts)
         window (w/window-extend-left last-interval trailing-n)
         bar-ds (m/? (get-bars env opts window))]
     (if (= 0 (tc/row-count bar-ds))
       (throw (ex-info "empty-bars" {:asset (get-asset opts) :n trailing-n :calendar calendar :dt bar-ds :window window}))
       bar-ds))))

(defn get-trailing-bars-window [env opts last-interval]
  (m/sp
   (if-let [width (get-in opts [:window :width])]
     (let [bar-px (or (get opts :bar-px) 10)
           preload-n (or (get opts :preload-n) 0)
           trailing-n (-> (int (/ width bar-px))
                          (+ preload-n))]
       (info "trailing window!" (str "window width:" width " trailing# " trailing-n))
       (m/? (get-trailing-bars env (assoc opts :trailing-n trailing-n) last-interval)))
     (do
       (info "trailing window-no-width" opts)
       (m/? (get-trailing-bars env opts last-interval))))))

(defn remove-preload [opts ds]
  (if (get-in opts [:window :width])
    (let [preload-n (or (get opts :preload-n) 0)]
      (if (> preload-n 0)
        (tc/select-rows ds (range preload-n (tc/row-count ds)))
        ds))
    ds))

#_(defn get-bars-lower-timeframe [env spec lower-timeframe]
    (let [calendar (get-calendar spec)
          market (first calendar)
          calendar-lower [market lower-timeframe]
          asset (s/get-asset spec)
          time (get-calendar-time env calendar)
          window (get-bar-window calendar time)]
      (get-bars {:asset asset
                 :calendar calendar-lower} window)))



