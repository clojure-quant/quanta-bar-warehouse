(ns dev.bardb.duck-generator
  (:require
   [missionary.core :as m]
   [tablecloth.api :as tc]
   [tmducken.duckdb :as tmduck]
   [quanta.bar.protocol :as b]
   [quanta.bar.db.duck :as duck]
   [quanta.bar.db.duck.admin :refer [checkpoint query]]))

(defn generate-bars [{:keys [bardb rand]} {:keys [asset calendar start end]}]
  (m/sp
   (let [bar-ds (m/? (b/get-bars rand
                                 {:asset asset
                                  :calendar calendar}
                                 {:start start
                                  :end end}))]
     (m/? (b/append-bars bardb {:asset asset
                                :calendar calendar}
                         bar-ds)))))

(defn db [label]
  (duck/start-bardb-duck (str "./duck-perf/" label ".ddb ")))

(def alphabet "abcdefghijklmnopqrstuvwxyz")

(defn asset-symbol [n]
  (let [r (java.util.Random. (long n))]
    (apply str
           (repeatedly 5
                       #(nth alphabet (.nextInt r (count alphabet)))))))

(defn generate-bars-assets [ctx {:keys [asset-n label] :as opts}]
  (let [opts (dissoc opts :asset-n :label)
        bardb (db label)
        ctx (assoc ctx :bardb bardb)]
    (m/sp
     (->>  (range asset-n)
           (map asset-symbol)
           (map (fn [asset]
                  (m/? (generate-bars ctx (assoc opts :asset asset)))
                  nil))
           doall)
     (checkpoint bardb)
     (duck/stop-bardb-duck bardb)
     (str "generated bars for " asset-n "assets in db: " label))))

(defn load-bars-assets [{:keys [asset-n calendar label]}]
  (println "load-bars outside thread: " (.getId (Thread/currentThread)))
  (m/sp
   (println "load-bars inside thread: " (.getId (Thread/currentThread)))
   (let [bardb (-> (db label)
                   ;(with-new-conn)
                   )
         row-count (->>  (range asset-n)
                         (map asset-symbol)
                         (map (fn [asset]
                                (println "q asset: " asset " calendar: " calendar)
                                (let [bar-ds (m/? (b/get-bars bardb {:asset asset :calendar calendar} {}))]
                                  (println "bar-ds: " bar-ds)
                                    ;(tc/row-count bar-ds)
                                  0)))
                         (reduce + 0.0))]
     (duck/stop-bardb-duck bardb)
       ;(str "loaded bars for " asset-n "assets in db: " label)
     row-count)))