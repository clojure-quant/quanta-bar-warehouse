(ns dev.bardb.bardb-duck-delete
  (:require
   [tick.core :as t]
   [tablecloth.api :as tc]
   [quanta.bar.protocol :as b]
   [quanta.bar.db.duckdb :as duck]
   [modular.system]))

(def ddb (modular.system/system :bardb-dynamic))

(def db (modular.system/system :duckdb))

(duck/delete-bars db [:crypto :d] "ETHUSDT")
