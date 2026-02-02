(ns quanta.bar.transform.dynamic.logger)

(def log-a (atom []))

(defn import-on-demand [opts window tasks]
  (when (seq tasks)
    (let [req (merge (select-keys opts [:asset :import :calendar])
                     (select-keys window [:start :end]))
          req (assoc req :tasks tasks)]
      (swap! log-a conj req))))

(comment
  (->> @log-a
       (filter #(= :eodhd (:import %))))

;
  )
