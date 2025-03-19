(ns quanta.bar.env
  (:require
   [taoensso.timbre :refer [trace debug info warn error]]
   [missionary.core :as m]
   [de.otto.nom.core :as nom]
   [tick.core :as t]
   [tablecloth.api :as tc]
   [quanta.calendar.window :as w]
   [quanta.calendar.interval :as i]
   [ta.db.bars.protocol :as b]
   [ta.db.bars.aligned :as aligned]))

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

(defn get-bars-aligned-filled
  "returns bars for asset/calendar/window"
  [env {:keys [asset calendar] :as opts} calendar-seq]
  (assert asset "cannot get-bars for unknown asset!")
  (assert calendar "cannot get-bars for unknown calendar!")
  (assert calendar-seq "cannot get-bars-aligned for unknown window!")
  (aligned/get-bars-aligned-filled (get-bar-db env) opts calendar-seq))

(defn get-trailing-bars [env {:keys [trailing-n] :as opts} last-interval]
  (info "get-trailing" trailing-n " bars ending: " (-> last-interval i/current :close))
  (m/sp
   (let [calendar (get-calendar opts)
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

(defn get-multiple-bars [env {:keys [assets] :as opts} cal-seq]
  (let [get-bars (fn [asset]
                   (info "loading: " asset)
                   (-> (get-bars-aligned-filled env (assoc opts :asset asset) cal-seq)
                       (tc/add-column :asset asset)))
        asset-map-seq (map (fn [asset]
                             {:asset asset
                              :bars (get-bars asset)}) assets)
        assets-bad (->> (filter #(nom/anomaly? (:bars %)) asset-map-seq)
                        (map :asset))
        assets-good (->> (remove #(nom/anomaly? (:bars %)) asset-map-seq)
                         (map :asset))
        bars-good (->> (remove #(nom/anomaly? (:bars %)) asset-map-seq)
                       (map :bars))]
    {:bad assets-bad
     :good assets-good
     :bars bars-good}))

(defn get-multiple-bars-trailing [env {:keys [calendar assets trailing-n] :as opts} last-interval]
  (let [window (w/window-extend-left last-interval trailing-n)]
    (get-multiple-bars env opts window)))

