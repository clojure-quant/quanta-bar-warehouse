(ns quanta.bar.db.duck
  (:require
   [taoensso.timbre :as timbre :refer [debug info warn error]]
   [clojure.java.io :as java-io]
   [missionary.core :as m]
   [tmducken.duckdb :as duckdb]
   [ta.db.bars.protocol :refer [bardb barsource]]
   [quanta.bar.db.duck.get-bars :refer [get-bars]]
   [quanta.bar.db.duck.append-bars :refer [append-bars]]
   [quanta.bar.db.duck.delete :refer [delete-bars]]
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

(defrecord bardb-duck [db conn new?]
  barsource
  (get-bars [this opts window]
    (info "get-bars " (select-keys opts [:task-id :asset :calendar :import]) window)
    (m/via m/blk (get-bars this opts window)))
  bardb
  (append-bars [this opts ds-bars]
    (m/via m/blk (append-bars  this opts ds-bars))))

(defn start-bardb-duck [opts]
  (let [{:keys [db conn new?] :as this} (duckdb-start-impl opts)]
    (when new?
      (init-tables! this))
    (bardb-duck. db conn new?)))

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

