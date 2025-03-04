(ns dev.bargenerator.randomfeed
  (:require [missionary.core :as m]))

(defn new-price [price]
  (let [chg (- (rand 1) 0.5)
        chg-prct (/ chg 100.0)]
    (* price (- 1.0 chg-prct))))

(defn trade-producer [asset start-price max-delay-ms]
  (m/ap
   (loop [price start-price]
     (m/? (m/sleep (rand-int max-delay-ms)))
     (let [price (new-price price)]
       (m/amb
        {:asset asset
         :price price
         :volume (rand-int 100)}
        (recur price))))))

(comment
  (def a (trade-producer "A" 100.0 10))

  (m/?
   (m/reduce conj [] (m/eduction (take 1000) a)))

;  
  )
(defn mix
  "Return a flow which is mixed by flows"
  ; will generate (count flows) processes, 
  ; so each mixed flow has its own process
  [& flows]
  (m/ap (m/?> (m/?> (count flows) (m/seed flows)))))

(def trade-feed
  (mix (trade-producer "A" 1.0 3000)
       (trade-producer "B" 10.0 3000)
       (trade-producer "C" 100.0 3000)))

(comment
  (m/?
   (m/reduce conj []
             (m/eduction (take 5) trade-feed)))

  ;[{:volume 34, :asset "B", :price 9.98609337717243}
  ; {:volume 4, :asset "B", :price 9.943237802872938}
  ; {:volume 97, :asset "A", :price 1.0038424574580802}
  ; {:volume 31, :asset "C", :price 100.41281594563398}
  ; {:volume 45, :asset "B", :price 9.928846880638302}]
  ;
  )


