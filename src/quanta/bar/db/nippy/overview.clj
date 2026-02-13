(ns quanta.bar.db.nippy.overview
  (:require
   [babashka.fs :as fs]
   [tablecloth.api :as tc]))

(defn extract-meta [filename]
  (let [re #"^(.+)-([^-]+)-([^-]+)\.nippy\.gz$"
        [_ asset market interval] (re-matches re filename)]
    {:asset asset
     :market (keyword market)
     :interval (keyword interval)}))

(defn get-existing-assets [path [market interval]]
  (->> (fs/list-dir path "*.nippy.gz")
       (map fs/file-name)
       (map extract-meta)
       (filter #(and (= (:market %) market) (= (:interval %) interval)))
       (map :asset)))

(defn create-summary [concatenated-ds]
  (when concatenated-ds
    (-> concatenated-ds
        (tc/group-by [:asset])
        (tc/aggregate {:count tc/row-count
                       :start (fn [ds]
                                (->> ds
                                     :date
                                     first))
                       :end (fn [ds]
                              (->> ds
                                   :date
                                   last))
                       :low (fn [ds]
                              (->> ds
                                   :close
                                   (apply min)))
                       :high (fn [ds]
                               (->> ds
                                    :close
                                    (apply max)))})
        (tc/select-columns  [:asset :start :end :count :low :high]))))

(comment
  (extract-meta "VFH-us-d.nippy.gz")
  (get-existing-assets "/home/florian/quantastore/bardb/eodhd-nippy/" [:us :d])

 ; 
  )