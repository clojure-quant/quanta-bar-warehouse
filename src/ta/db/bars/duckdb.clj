(ns ta.db.bars.duckdb
  (:require
   [taoensso.timbre :as timbre :refer [debug info warn error]]
   [tick.core :as t]
   [tablecloth.api :as tc]
   [clojure.java.io :as java-io]
   [tmducken.duckdb :as duckdb]
   [ta.db.bars.protocol :refer [bardb barsource]]
   [ta.db.bars.duckdb.get-bars :refer [get-bars]]
   [ta.db.bars.duckdb.append-bars :refer [append-bars]]
   [ta.db.bars.duckdb.delete :refer [delete-bars]]
   [ta.db.bars.duckdb.calendar :refer [bar-category->table-name]]
   [ta.db.bars.duckdb.table :refer [init-tables]]))

;; https://github.com/techascent/tmducken

(defn- exists-db? [path]
  (.exists (java-io/file path)))

(defn- duckdb-start-impl [db-filename]
  (duckdb/initialize! {:duckdb-home "./binaries"})
  (let [new? (exists-db? db-filename)
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
    (get-bars this opts window))
  bardb
  (append-bars [this opts ds-bars]
    ;(info "this: " this)
    (append-bars  this opts ds-bars)))

(defn start-bardb-duck [opts]
  (let [{:keys [db conn new?]} (duckdb-start-impl opts)]
    (bardb-duck. db conn new?)))

(defn stop-bardb-duck [state]
  (duckdb-stop-impl state))

(comment
  (-> (now) type)

  (require '[modular.system])
  (def db (:duckdb modular.system/system))
  (def db (duckdb-start-impl "../../output/duckdb/bars"))
  db

  (require '[tech.v3.dataset :as ds])
  (def stocks
    (ds/->dataset "https://github.com/techascent/tech.ml.dataset/raw/master/test/data/stocks.csv"
                  {:key-fn keyword
                   :dataset-name :stocks}))
  stocks
  (tc/info stocks)

  (t/inst time)
  (format-date time)

  (str time)

  (t/inst "2023-01-01 0:0:0")

  (now)
  empty-ds
  (tc/info empty-ds)
  (init-tables db)

  (duckdb/create-table! (:conn db) empty-ds)
  (duckdb/insert-dataset! (:conn db) empty-ds)

  (exists-db?  "../../output/duckdb/bars")

  (duckdb/insert-dataset! db stocks)
  (ds/head (duckdb/sql->dataset db "select * from stocks"))
  (def stmt (duckdb/prepare db "select * from stocks "))
  (stmt)

  (def r (stmt))

  r

;
  )

