(ns quanta.bar.split.adjust
  (:require
   [tick.core :as t]
   [tablecloth.api :as tc]
   [tech.v3.tensor :as dtt]
   [tech.v3.datatype.functional :as dfn]))

(defn vec-const [size val]
  (-> (vec (repeat size val))
      (dtt/->tensor)))

(defn add-split-factor-linear
  "bars:   dataset, ascending by :date
   splits: dataset, ascending by :date, with :factor

   Backward rule:
   - last bar factor = 1.0
   - walking backward: when bar date == current split-date, factor *= split-factor
   - each bar row gets the current factor"
  [bars splits]
  (let [bar-dates   (:date bars)
        split-dates (:date splits)
        split-facs  (:factor splits)

        nb (count bar-dates)
        ns (count split-dates)

        ;; output column (float64)
        out (vec-const nb 1.0)]

    (loop [bar-idx (dec nb)          ;; bar idx from end
           split-idx (dec ns)          ;; split idx from end
           cum-factor 1.0]

      (let [d (bar-dates bar-idx)
            _ (println "bar-idx: " bar-idx " date: " d)
              ;; apply all splits that happen exactly on this bar date
              ;; (handles multiple splits same day)
            [split-idx cum-factor] (loop [split-idx-new split-idx
                                          cum-factor-new  cum-factor]
                                     (if
                                       ; new split 
                                      (and (>= split-idx-new 0)
                                           (t/> (split-dates split-idx-new) d))
                                       (recur (dec split-idx-new) (* cum-factor-new (split-facs split-idx-new)))
                                       ; keep existing value
                                       [split-idx-new cum-factor-new]))]
        (dtt/mset! out bar-idx cum-factor)
        (when (> bar-idx 0)
          (recur (dec bar-idx) split-idx cum-factor))))
    (tc/add-column bars :factor out)))

(defn has-col [ds col]
  (->> ds
       tc/columns
       (map meta)
       (filter #(= col (:name %)))
       seq
       ;(map :name)
       ))

(defn split-adjust [bar-ds split-ds]
  (let [{:keys [open high low close factor] :as ds} (add-split-factor-linear bar-ds split-ds)]
    (tc/add-columns ds
                    {:open (dfn// open factor)
                     :high (dfn// high factor)
                     :low (dfn// low factor)
                     :close (dfn// close factor)
                     :volume (dfn/* close factor)})))


