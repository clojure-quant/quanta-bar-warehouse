(ns quanta.bar.db.duck
  (:require
   [taoensso.timbre :as timbre :refer [debug info error]]
   [clojure.java.io :as java-io]
   [babashka.fs :as fs]
   [tmducken.duckdb :as duckdb]
   [quanta.calendar.window :refer [window->close-range]]
   [quanta.bar.protocol :refer [bardb barsource]]
   [quanta.bar.db.duck.impl.pool :refer [start-pool-actor with-conn]]
   [quanta.bar.db.duck.impl.get-bars :refer [get-bars]]
   [quanta.bar.db.duck.impl.append-bars :refer [append-bars]]
   [quanta.bar.db.duck.impl.delete :refer [delete-bars]]
   [quanta.bar.db.duck.impl.warehouse :refer [warehouse-summary]]
   [quanta.bar.db.duck.impl.table :refer [init-tables!]]))

;; https://github.com/techascent/tmducken

(defn- exists-db? [db-filename]
  (.exists (java-io/file db-filename)))

(defn find-duckdb-so []
  (or (System/getenv "DUCKDB_LIB_DIR") "./binaries"))

(defn- duckdb-start-impl [db-filename]
  (let [duckdb-home (find-duckdb-so)]
    (info "starting duckdb with duck-db-lib dir: " duckdb-home)
    (when-not (fs/exists? duckdb-home)
      (throw (ex-info "duckdb-dll-path not found. Set DUCKDB_LIB_DIR env var or ./binaries " {})))
    (duckdb/initialize! {:duckdb-home duckdb-home})
    (let [new? (not (exists-db? db-filename))
          db (duckdb/open-db db-filename)
          conn (duckdb/connect db)]
      {:db db
       :conn conn
       :pool (start-pool-actor {:db db :size 8})
       :new? new?})))

(defn- duckdb-stop-impl [{:keys [conn] :as session}]
  (try
    (info "duck-db stop. session: " (keys session))
    (duckdb/disconnect conn)
    (catch Exception ex
      (error "duck-db stop exception: " (ex-message ex)))))

(defrecord bardb-duck [db conn new? pool]
  barsource
  (get-bars [_ opts window]
    (let [; allow to pass in a calendar/window which does not have :start :end
          window (if (:window window) (window->close-range window) window)]
      (with-conn pool (get-bars c opts window))))
  bardb
  (append-bars [_ opts bar-ds]
    (with-conn pool (append-bars c opts bar-ds)))
  (delete-bars [_ opts]
    (with-conn pool (delete-bars c (:calendar opts) (:asset opts))))
  (summary [_ opts]
    (with-conn pool (warehouse-summary c (:calendar opts)))))

(defn start-bardb-duck [opts]
  (let [{:keys [db conn new? pool]} (duckdb-start-impl opts)]
    (when new?
      (init-tables! conn))
    (bardb-duck. db conn new? pool)))

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

