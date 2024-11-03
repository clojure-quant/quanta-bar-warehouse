(ns quanta.bar.env
  (:require
   [taoensso.timbre :refer [trace debug info warn error]]
   [missionary.core :as m]
   [de.otto.nom.core :as nom]
   [tick.core :as t]
   [tablecloth.api :as tc]
   [ta.calendar.core :refer [trailing-window #_get-bar-window]]
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

(defn- get-trailing-n [spec]
  (:trailing-n spec))

(defn get-bars
  "returns bars for asset/calendar/window"
  [env opts window]
  (let [calendar (:calendar opts)
        asset (:asset opts)]
    (info "get-bars: " window)
    (assert asset "cannot get-bars for unknown asset!")
    (assert calendar "cannot get-bars for unknown calendar!")
    (assert window "cannot get-bars for unknown window!")
    (b/get-bars (get-bar-db env) opts window)))

(defn get-bars-aligned-filled
  "returns bars for asset/calendar/window"
  [env {:keys [asset calendar] :as opts} calendar-seq]
  (assert asset "cannot get-bars for unknown asset!")
  (assert calendar "cannot get-bars for unknown calendar!")
  (assert calendar-seq "cannot get-bars-aligned for unknown window!")
  (aligned/get-bars-aligned-filled (get-bar-db env) opts calendar-seq))

#_(defn get-calendar-time [env calendar]
    (let [calendar-time (:calendar-time env)]
      (assert calendar-time "environment does not provide calendar-time!")
      (get @calendar-time calendar)))

(defn calendar-seq->window [calendar-seq]
  (let [dend  (first calendar-seq)
        dstart (last calendar-seq)
        dend-instant (t/instant dend)
        dstart-instant (t/instant dstart)]
    {:start dstart-instant
     :end dend-instant}))

(defn get-trailing-bars [env opts bar-close-date]
  (info "get-trailing-bars " bar-close-date)
  (m/sp
   (let [trailing-n (get-trailing-n opts)
         calendar (get-calendar opts)
         calendar-seq (trailing-window calendar trailing-n bar-close-date)
         window (calendar-seq->window calendar-seq)
         bar-ds (m/? (get-bars env opts window))]
     (if (= 0 (tc/row-count bar-ds))
       (throw (ex-info "empty-bars" {:asset (get-asset opts) :n trailing-n :calendar calendar :dt bar-ds :window window}))
       bar-ds))))

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

(defn get-multiple-bars-trailing [env {:keys [calendar assets trailing-n] :as opts} end-dt]
  (let [cal-seq (trailing-window calendar trailing-n end-dt)]
    (get-multiple-bars env opts cal-seq)))

