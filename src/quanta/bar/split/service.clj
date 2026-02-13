(ns quanta.bar.split.service
  (:require
   [missionary.core :as m]
   [tablecloth.api :as tc]
   [quanta.bar.protocol :as b]
   [quanta.bar.split.adjust :refer [split-adjust]]))

(defn save-splits [{:keys [bardb prefix]} split-ds]
  (let [prefix-asset (fn [asset] (str prefix asset))]
    (m/sp
     (->> (-> split-ds
              (tc/rename-columns {:factor :close})
              (tc/add-columns {:asset (map prefix-asset (:asset split-ds))
                               :open (:factor split-ds)
                               :high (:factor split-ds)
                               :low (:factor split-ds)
                               :volume 0}))
          (b/append-bars bardb {#_:asset #_"XXX" :calendar [:us :d]})
          (m/?)))))

(defn delete-splits [{:keys [bardb prefix]} asset]
  (let [prefix-asset (fn [asset] (str prefix asset))]
    (b/delete-bars bardb {:asset (prefix-asset asset) :calendar [:us :d]})))

(defn get-splits [{:keys [bardb prefix]} asset]
  (let [prefix-asset (fn [asset] (str prefix asset))]
    (m/sp
     (->> (-> (b/get-bars bardb {:asset (prefix-asset asset) :calendar [:us :d]} {})
              (m/?)
              (tc/rename-columns {:close :factor})
              (tc/add-column :asset asset)
              (tc/select-columns [:asset :date :factor]))))))

(defrecord barsource-splitadjust [bardb prefix]
  b/barsource
  (get-bars [_this opts window]
    (m/sp
     (let [bar-ds (m/? (b/get-bars bardb opts window))
           split-ds (m/? (get-splits {:bardb bardb :prefix prefix} (:asset opts)))]
       (if (= 0 (tc/row-count bar-ds))
         bar-ds
         (split-adjust bar-ds split-ds))))))

(defn start-split-service
  "split service allows to save/delete/get splits
   and provides a barsource that returns split-adjusted data."
  [{:keys [bardb prefix]
    :or {prefix "_split_"}}]
  {:bardb bardb
   :prefix prefix
   :barsource (barsource-splitadjust. bardb prefix)})


