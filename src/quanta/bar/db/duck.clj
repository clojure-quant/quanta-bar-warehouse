(ns quanta.bar.db.duck
  (:require
   [taoensso.timbre :as timbre :refer [debug info warn error]]
   [clojure.java.io :as java-io]
   [missionary.core :as m]
   [tmducken.duckdb :as duckdb]
   [ta.db.bars.protocol :refer [bardb barsource]]
   [quanta.bar.db :refer [bar-db]]
   [quanta.bar.db.duck.get-bars :refer [get-bars]]
   [quanta.bar.db.duck.append-bars :refer [append-bars]]
   [quanta.bar.db.duck.delete :refer [delete-bars]]
   [quanta.bar.db.duck.warehouse :refer [warehouse-summary]]
   [quanta.bar.db.duck.table :refer [init-tables!]]))

;; https://github.com/techascent/tmducken

(defn- exists-db? [db-filename]
  (.exists (java-io/file db-filename)))

(defn- duckdb-start-impl [db-filename]
  (duckdb/initialize! {:duckdb-home "./binaries"})
  (let [new? (not (exists-db? db-filename))
        db (duckdb/open-db db-filename)
        conn (duckdb/connect db)]
    {:db db
     :conn conn
     :new? new?}))

(defn- duckdb-stop-impl [{:keys [db conn] :as session}]
  (duckdb/disconnect conn))

;; CREATE INDEX s_idx ON films (revenue);

(defrecord bardb-duck [db conn new? lock]
  barsource
  (get-bars [this opts window]
    (m/sp
     (m/holding
      lock
      (info "get-bars " (select-keys opts [:task-id :asset :calendar :import]) window)
      (m/? (m/via m/blk (get-bars this opts window))))))
  bardb
  (append-bars [this opts ds-bars]
    (m/via m/blk (m/holding lock (append-bars  this opts ds-bars))))
  bar-db
  (summary [this opts]
    (m/via m/blk
           (m/holding lock
                      (warehouse-summary this (:calendar opts)))))
  (delete-bars [this opts]
    (m/via m/blk
           (m/holding lock
                      (delete-bars this (:calendar opts) (:asset opts))))))

(defn start-bardb-duck [opts]
  (let [{:keys [db conn new?] :as this} (duckdb-start-impl opts)
        lock (m/sem)]
    (when new?
      (init-tables! this))
    (bardb-duck. db conn new? lock)))

(defn stop-bardb-duck [state]
  (duckdb-stop-impl state))

(comment

  (ds/head (duckdb/sql->dataset db "select * from stocks"))
  (def stmt (duckdb/prepare db "select * from stocks "))
  (stmt)

  (def r (stmt))

  r

;
  )

