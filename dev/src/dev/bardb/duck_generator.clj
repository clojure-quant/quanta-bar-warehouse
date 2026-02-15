(ns dev.bardb.duck-generator
  (:require
   [missionary.core :as m]
   [tablecloth.api :as tc]
   [quanta.bar.protocol :as b]
   [quanta.bar.db.duck :as duck]
   [quanta.bar.db.random :refer [start-random-bar-source]]))

(def rs (start-random-bar-source {:seed 42
                                  :zero-price 100.0}))

(defn db [label]
  (duck/start-bardb-duck (str "./duck-perf/" label ".ddb ")))

(def alphabet "abcdefghijklmnopqrstuvwxyz")

(defn asset-symbol [n]
  (let [r (java.util.Random. (long n))]
    (apply str
           (repeatedly 5
                       #(nth alphabet (.nextInt r (count alphabet)))))))

(defn generate-bars [bardb {:keys [asset calendar start end]}]
  (m/sp
   (let [bar-ds (m/? (b/get-bars rs
                                 {:asset asset
                                  :calendar calendar}
                                 {:start start
                                  :end end}))]
     (m/? (b/append-bars bardb {:asset asset
                                :calendar calendar}
                         bar-ds)))))

(defn generate-bars-assets [{:keys [asset-n label] :as opts}]
  (let [opts (dissoc opts :asset-n :label)
        bardb (db label)]
    (m/sp
     (->>  (range asset-n)
           (map asset-symbol)
           (map (fn [asset]
                  (m/? (generate-bars bardb (assoc opts :asset asset)))
                  nil))
           doall)
     ;(checkpoint bardb)
     (duck/stop-bardb-duck bardb)
     (str "generated bars for " asset-n "assets in db: " label))))

(defn process [f]
  (m/ap  (m/? (m/?> f))))

(defn load-bars-assets [{:keys [asset-n calendar label]}]
  (m/sp
   (let [bardb  (db label)
         assets (->> (range asset-n) (map asset-symbol))
         assets-t  (map (fn [asset] (b/get-bars bardb {:asset asset :calendar calendar} {})) assets)
         bar-ds-f (process (m/seed assets-t))
         bar-count (m/? (m/reduce (fn [{:keys [asset-n bar-n]} ds]
                                    {:asset-n (inc asset-n)
                                     :bar-n (+ bar-n (tc/row-count ds))}) {:asset-n 0 :bar-n 0} bar-ds-f))]
     (duck/stop-bardb-duck bardb)
       ;(str "loaded bars for " asset-n "assets in db: " label)
     bar-count)))


