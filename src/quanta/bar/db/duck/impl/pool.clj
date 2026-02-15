(ns quanta.bar.db.duck.impl.pool
  (:require
   [missionary.core :as m]
   [tmducken.duckdb :as tmdb]
   [quanta.bar.db.duck.impl.admin :refer [set-threads]]))

(defn start-pool-actor
  "Returns {:self mbx :acquire (fn [] task) :release (fn [c] nil) :stop (fn [] nil)}"
  [{:keys [db size]
    :or   {size 8}}]
  (let [on-create (fn [c]
                    (set-threads c size)
                    nil)
        ;_ (println "pool actor starting..")
        self (m/mbx)
        ;; create connections once
        conns (vec (repeatedly size #(doto (tmdb/connect db) on-create)))
        ;; actor loop (runs forever unless you add a :stop protocol)
        _cancel!
        ((m/sp
          ;(println "pool-actor loop start! conns: " conns)
          (loop [avail conns
                 waiters clojure.lang.PersistentQueue/EMPTY]
            (let [{:keys [op reply conn]} (m/? self)]
              ;(println "processing op: " op " waiting: " (count waiters) " avail: " (count avail))
              (case op
                :acquire
                (if (seq avail)
                  (do
                    (reply (peek avail))              ; dfv assign (1-arity)
                    (recur (pop avail) waiters))
                  (recur avail (conj waiters reply))) ; queue the dfv
                
                :release
                (if (seq waiters)
                  (let [w (peek waiters)]
                    (w conn)                          ; hand conn to oldest waiter
                    (recur avail (pop waiters)))
                  (recur (conj avail conn) waiters))

                 ;; optional: stop (disconnect everything still in pool)
                :stop
                (do
                  (doseq [c avail] (tmdb/disconnect c))
                   ;; if you want, also fail pending waiters (requires a convention)
                  nil)

                 ;; unknown op -> ignore
                (recur avail waiters)))))
         (fn [_] nil)
         (fn [e] (throw e)))]

    {:self self

     ;; acquire returns a TASK (so callers can (m/? (acquire)) inside m/sp)
     :acquire
     (fn []
       (m/sp
        ;(println "acquire need")
        (let [r (m/dfv)]
          (self {:op :acquire :reply r}) ; post message
          (m/? r)      ; wait for assigned conn
          )))               

     ;; release is fire-and-forget
     :release
     (fn [c]
       ;(println "release conn!")
       (self {:op :release :conn c})
       nil)

     ;; optional stop
     :stop
     (fn []
       (self {:op :stop})
       nil)}))

(defmacro with-conn [pool body]
  `(m/sp
    (let [~'c (m/? ((:acquire ~pool)))]
      (try
        (m/? (m/via m/blk ~body))
        (finally
          ((:release ~pool) ~'c))))))
