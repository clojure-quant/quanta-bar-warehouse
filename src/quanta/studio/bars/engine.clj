(ns quanta.studio.bars.engine
  (:require
   [taoensso.timbre :as timbre :refer [debug info warn error]]
   [de.otto.nom.core :as nom]
   [ta.db.bars.protocol :as b :refer [barsource bardb]]))

(defrecord bar-engine [imports dbs transforms]
  barsource
  (get-bars [this {:keys [import bardb transform] :as opts} window]
    (if (not transform)
      (let [opts-clean (dissoc opts :import :bardb :transform)
            bar-source (cond
                         import (get imports import)
                         bardb (get dbs bardb)
                         :else nil)]
        (if bar-source
          (b/get-bars bar-source opts-clean window)
          (nom/fail :bar-loader-source-not-found
                    {:message (str "bar-loader source unknown import: " import " bardb: " bardb)})))
      (let [bar-transformer (get transforms transform)
            opts-clean (-> opts
                           (dissoc :transform)
                           (assoc :engine this))]
        (if bar-transformer
          (b/get-bars bar-transformer opts-clean window)
          (nom/fail :bar-loader-transformer-not-found
                    {:message (str "bar-loader transformer unknown transformer: " transform)})))))
  bardb
  (append-bars [this {:keys [bardb] :as opts} bar-ds]
    (let [db (get dbs bardb)
          opts-clean (dissoc opts :import :bardb :transform)]
      (if db
        (b/append-bars db opts-clean bar-ds)
        (do (error "could not save " opts " bar-db unknown!")
            (nom/fail :bar-loader-db-not-found
                      {:message (str "bar-loader unknown bardb: " bardb)}))))))

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
  (bar-engine. import bardb transform))