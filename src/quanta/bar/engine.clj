(ns quanta.bar.engine
  (:require
   [taoensso.timbre :as timbre :refer [debug info warn error]]
   [missionary.core :as m]
   [ta.db.bars.protocol :as b :refer [barsource bardb]]))

(defrecord bar-engine [imports dbs transforms]
  barsource
  (get-bars [this {:keys [import bardb transform] :as opts} window]
    (m/sp
     (cond
       transform
       (if-let [bar-transformer (get transforms transform)]
         (m/? (b/get-bars bar-transformer (-> opts
                                              (dissoc :transform)
                                              (assoc :engine this)) window))
         (throw (ex-info "unknown-transform" {:transform transform
                                              :opts opts
                                              :window window})))

       import
       (if-let [importer (get imports import)]
         (m/? (b/get-bars importer (dissoc opts :import :bardb :transform) window))
         (throw (ex-info "unknown-importer" {:import import
                                             :opts opts
                                             :window window})))
       bardb
       (if-let [bardb (get dbs bardb)]
         (m/? (b/get-bars bardb (dissoc opts :import :bardb :transform) window))
         (throw (ex-info "unknown-bardb" {:import import
                                          :opts opts
                                          :window window})))
       :else
       (throw (ex-info "missing-import-bardb-transform" {:opts opts
                                                         :window window})))))
  bardb
  (append-bars [this {:keys [bardb] :as opts} bar-ds]
    (if-let [db (get dbs bardb)]
      (b/append-bars db (dissoc opts :import :bardb :transform) bar-ds)
      (throw (ex-info "unknown-bardb" {:bardb bardb
                                       :opts opts})))))

(defrecord bar-task [base]
  barsource
  (get-bars [_ opts window]
    (m/sp
     (b/get-bars base opts window)))
  bardb
  (append-bars [_ opts bar-ds]
    (b/append-bars base opts bar-ds)))

(defn wrap-task [feeds]
  (->> feeds
       (map (fn [[k v]]
              [k (bar-task. v)]))
       (into {})))

(defn start-bar-engine
  "implements barsource and bardb protocols to load and save bars.
   depending of the following extra keys in :opts the action is different:
   :import import bars from a bar-import feed.
   :bardb loads bars from a bar-db
   :transform this works together with :import and :bardb and 
   applies a transformation, like :compress :dynamic or :shuffle"
  [{:keys [import bardb transform]
    :or {import {}
         bardb {}
         transform {}}}]
  (info "starting bar-loader with "
        (count import) " importers"
        (count bardb) " dbs"
        (count transform) " transformers")
  (bar-engine. import (wrap-task bardb) transform))
