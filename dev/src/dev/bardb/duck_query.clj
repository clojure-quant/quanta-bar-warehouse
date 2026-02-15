(ns dev.bardb.duck-query
  (:require
   [missionary.core :as m]
   [tablecloth.api :as tc]
   [tick.core :as t]
   [quanta.bar.protocol :as b]
   [quanta.bar.db.duck :as duck]
   [quanta.bar.db.duck.impl.admin :refer [checkpoint query]]
   [dev.bardb.duck-generator :refer [generate-bars]]))

(def bardb (duck/start-bardb-duck "./duck-perf/test2.ddb"))

bardb

(m/? (generate-bars bardb
                    {:asset "BONGO"
                     :calendar [:us :d]
                     :start (t/instant "2005-01-01T00:00:00Z")
                     :end  (t/instant "2010-03-01T20:00:00Z")}))

(m/? (b/get-bars bardb  {:asset "BONGO" :calendar [:us :d]} {}))

(m/? (b/summary bardb {:calendar [:us :d]}))

(checkpoint (:conn bardb))

(duck/stop-bardb-duck bardb)

(query (:conn bardb) "SELECT schema_name FROM information_schema.schemata;")
(query (:conn bardb) "PRAGMA show_tables;")

(query (:conn bardb) "SELECT  column_name, data_type, is_nullable
              FROM information_schema.columns
              WHERE table_name = 'crypto_d'
              ORDER BY ordinal_position;")

(query (:conn bardb) "CREATE TABLE prices_sorted AS
              SELECT * FROM crypto_d
              ORDER BY asset, date;")

(query (:conn bardb) "SELECT column_name, data_type, is_nullable
              FROM information_schema.columns
              WHERE table_name = 'prices_sorted'
              ORDER BY ordinal_position;")

(query (:conn bardb) "PRAGMA table_info('crypto_d');")