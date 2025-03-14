(ns quanta.bar.db.duck.ds
  (:require
   [clojure.set :refer [subset?]]
   [tick.core :as t]
   [tablecloth.api :as tc]
   [tech.v3.dataset :as tds]
   [quanta.bar.db.duck.calendar :refer [bar-category->table-name]]))

(defn empty-ds [calendar]
  (let [table-name (bar-category->table-name calendar)]
    (-> (tc/dataset [{:asset "000"
                      :date (t/instant)
                      :open 0.0 :high 0.0 :low 0.0 :close 0.0
                      :volume 0.0 ; crypto volume is double.
                      :ticks 0}])
        (tc/set-dataset-name table-name))))

(defn order-columns [ds]
  ; see: https://github.com/techascent/tmducken/issues/18
  ; column order needs to be identical as in empty-ds
  (tds/select-columns ds [:asset :date :open :high :low :close :volume :ticks]))

(defn order-columns-old [ds]
  ; see: https://github.com/techascent/tmducken/issues/18
  ; column order needs to be identical as in empty-ds
  ; possibly this thing leads to the warnings
  (tc/dataset [(:asset ds)
               (:date ds)
               (:open ds)
               (:high ds)
               (:low ds)
               (:close ds)
               (:volume ds)
               (:ticks ds)]))

(defn- date-type [ds]
  (-> ds :date meta :datatype))

(defn- ensure-date-instant
  "duckdb needs one fixed type for the :date column.
   we use instant, which techml calls packed-instant"
  [ds]
  (let [t (date-type ds)
        instant? (= t :packed-instant)]
    (if instant?
      ds
      (tc/add-column ds :date (map t/instant (:date ds))))))

(defn- ensure-col-float64
  "duckdb needs one fixed type for the :volume column.
   many data-sources return volume as int, so we might have to convert it."
  [ds col]
  (let [t (-> ds col meta :datatype)]
    (if (= t :float64)
      ds
      (tc/add-column ds col (map double (col ds))))))

(defn- has-col [ds col]
  (->> ds
       tc/columns
       (map meta)
       (filter #(= col (:name %)))
       empty?
       not
       ;(map :name)
       ))

(defn- ensure-ticks [ds]
  (if (has-col ds :ticks)
    ds
    (tc/add-column ds :ticks 0)))

(defn- ensure-asset [ds asset]
  (if (has-col ds :asset)
    ds
    (tc/add-column ds :asset asset)))

(defn- ds-cols [ds]
  (->> ds tc/column-names (into #{})))

(defn- has-dohlcv? [ds]
  (subset? #{:date :open :high :low :close :volume} (ds-cols ds)))

(defn sanitize-ds [ds asset]
  (assert (has-dohlcv? ds) "ds needs to have columns [:date :open :high :low :close :volume]")
  (-> ds
      (ensure-date-instant)
      (ensure-col-float64 :volume)
      (ensure-col-float64 :open)
      (ensure-col-float64 :high)
      (ensure-col-float64 :low)
      (ensure-col-float64 :close)
      (ensure-ticks)
      (ensure-asset asset)
      (order-columns)))