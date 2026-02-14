(ns quanta.bar.db.random
  (:require
   [missionary.core :as m]
   [tick.core :as t]
   [tech.v3.datatype.functional :as dfn]
   [tablecloth.api :as tc]
   [quanta.calendar.core :as cal]
   [quanta.bar.protocol :as b])
  (:import
   [java.util Random]))

(defn- daily-return
  "Uniform random daily return in [-1%, +1%]."
  ^double
  [^Random rng]
  (+ 1.0 (- (* 0.02 (.nextDouble rng)) 0.01)))

(defn- seed-for-asset-calendar
  "Derives a per-(asset,calendar) seed from the bar-source base seed."
  ^long
  [^long seed asset calendar]
  (+ seed (long (hash [asset calendar]))))

(defn generate-series [{:keys [seed zero-date zero-price asset calendar start end]
                        :or {zero-date (t/instant "1980-01-01T00:00:00Z")
                             zero-price 100.0
                             seed 42}}]
  (let [dates (->> (cal/fixed-window calendar {:start zero-date :end end})
                   (map t/instant)
                   (reverse)
                   (into []))
        seed-asset (seed-for-asset-calendar seed asset calendar)
        rng (Random. (long seed-asset))
        rets (for [_i (range (count dates))]
               (daily-return rng))
        prices (reductions * zero-price rets)]
    (-> (tc/dataset {:asset asset
                     :date dates
                     :close prices
                     :high (dfn/+ prices 0.1)
                     :low (dfn/- prices 0.1)
                     :open prices
                     :volume 0.0})
        (tc/select-rows #(t/>= (:date %) start)))))

(defrecord random-bar-source [seed zero-date zero-price]
  b/barsource
  (get-bars [_this {:keys [asset calendar] :as _opts} window]
    (m/sp
     (generate-series {:seed seed
                       :zero-price zero-price
                       :zero-date zero-date
                       :asset asset
                       :calendar calendar
                       :start (:start window)
                       :end (:end window)}))))

(defn start-random-bar-source
  "Creates a deterministic random bar source.
  - `seed` is the base seed.
  - zero-date the start date at which the series starts at zero-price. 
  This makes each asset/calendar stream distinct but still reproducible."
  [{:keys [seed zero-price zero-date]
    :or {zero-date (t/instant "1980-01-01T00:00:00Z")
         zero-price 100.0
         seed 42}}]
  (when (nil? seed)
    (throw (ex-info ":seed is required" {})))
  (random-bar-source. (long seed) zero-date (double zero-price)))
(comment
  
  (-> (seed-for-asset-calendar 5 "MOF" [:us :d])
      (Random.))
  
  (generate-series {:seed 501
                    :zero-date (t/instant "2000-01-01T00:00:00Z")
                    :zero-price 100.0
                    :asset "MSFT"
                    :calendar [:us :d]
                    :start (t/instant "2025-01-01T00:00:00Z")
                    :end (t/instant "2026-02-14T14:06:13Z")})
; 
  )




