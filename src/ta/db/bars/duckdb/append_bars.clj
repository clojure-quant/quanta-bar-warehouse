(ns ta.db.bars.duckdb.append-bars
  (:require
   [clojure.set :refer [subset?]]
   [taoensso.timbre :as timbre :refer [debug info warn error]]
   [tick.core :as t]
   [tablecloth.api :as tc]
   [tmducken.duckdb :as duckdb]
   [ta.db.bars.duckdb.calendar :refer [bar-category->table-name]]))

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

(defn- ensure-epoch [ds]
  (if (has-col ds :epoch)
    ds
    (tc/add-column ds :epoch 0)))

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

(defn order-columns [ds]
  (tc/dataset [(:date ds)
               (:asset ds)
               (:open ds)
               (:high ds)
               (:low ds)
               (:close ds)
               (:volume ds)
               (:epoch ds)
               (:ticks ds)]))

(defn order-columns-strange [ds]
  ; see: https://github.com/techascent/tmducken/issues/18
  (tc/dataset [(:open ds)
               (:epoch ds)
               (:date ds)
               (:close ds)
               (:volume ds)
               (:high ds)
               (:low ds)
               (:ticks ds)
               (:asset ds)]))

(defn sanitize-ds-before-append [ds asset]
  (assert (has-dohlcv? ds) "ds needs to have columns [:date :open :high :low :close :volume]")
  (-> ds
      (ensure-date-instant)
      (ensure-col-float64 :volume)
      (ensure-col-float64 :open)
      (ensure-col-float64 :high)
      (ensure-col-float64 :low)
      (ensure-col-float64 :close)
      (ensure-epoch)
      (ensure-ticks)
      (ensure-asset asset)
      (order-columns-strange)))

(defn append-bars [session {:keys [calendar asset]} ds]
  (let [ds (sanitize-ds-before-append ds asset)
        table-name (bar-category->table-name calendar)
        ds-with-name (tc/set-dataset-name ds table-name)]
    (info "duckdb append-bars asset: " asset " calendar: " calendar " bar:# " (tc/row-count ds))
    ;(info "duckdb append-bars ds-meta: " (tc/info ds))
    ;(info "session: " session)
    ;(info "ds: " ds)
    ;(info "date col type: " (date-type ds))
    (duckdb/insert-dataset! (:conn session) ds-with-name)))