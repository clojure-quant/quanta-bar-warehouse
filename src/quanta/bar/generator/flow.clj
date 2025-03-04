(ns quanta.bar.generator.flow
  (:require
   [missionary.core :as m]
   [tick.core :as t]
   [quanta.bar.generator.bar :refer [create-bars]]))

(defn take-until
  "like take-while, but includes the first element that 
   matches the predicate"
  ([pred]
   (fn [rf]
     (let [done? (volatile! false)]
       (fn
         ([] (rf))  ;; init
         ([result] (rf result))  ;; complete
         ([result input]  ;; step function
          (let [result (rf result input)]
            (if (pred input)
              (do (vreset! done? true)
                  (reduced result))
              result)))))))
  ([pred coll]
   (sequence (take-until pred) coll)))

(comment
  (def data [1 2 3 (t/instant) 4 5 6 (t/instant)])
  (take-while #(not (t/instant? %))  data)
  (take-until t/instant? data)
  (transduce (take-until t/instant?) conj data)
 ; 
  )

(defn time-buffered [clock-t data-f]
  (m/ap
   (let [restartable (second (m/?> (m/group-by {} data-f)))]
     (m/? (->> (m/ap
                (m/amb=
                 (m/? clock-t)
                 (m/?> restartable)))
               (m/eduction (take-until t/instant?))
               (m/reduce conj))))))

(defn trade-time-vec->bar [trade-time-vec]
  (let [trades (butlast trade-time-vec)
        dt  (last trade-time-vec)]
    (create-bars dt trades)))

(defn bar-f [clock-t trade-flow]
  (let [vec-flow (time-buffered clock-t trade-flow)]
    (m/eduction (map trade-time-vec->bar) vec-flow)
    ;vec-flow
    ))


