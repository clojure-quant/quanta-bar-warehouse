(ns dev.bardb.duck.query
  (:require
   [missionary.core :as m]
   [tick.core :as t]
   [quanta.bar.protocol :as b]
   [quanta.bar.db.duck :as duck]
   [quanta.bar.db.duck.impl.admin :refer [checkpoint query]]
   [quanta.bar.db.duck.impl.get-bars :as ibar]
   [dev.bardb.duck-generator :refer [generate-bars]]))

(def bardb (duck/start-bardb-duck "./duck-perf/test2.ddb"))

(m/? (generate-bars bardb
                    {:asset "BONGO"
                     :calendar [:us :d]
                     :start (t/instant "2005-01-01T00:00:00Z")
                     :end  (t/instant "2010-03-01T20:00:00Z")}))

(m/? (b/get-bars bardb  {:asset "BONGO" :calendar [:us :d]} {}))

(m/? (b/summary bardb {:calendar [:us :d]}))

;; SINGLE ASSET QUERY

(ibar/sql-bars-full [:us :d] "BONGO" false)
; "SELECT * from us_d WHERE asset = 'BONGO' order by date"

(ibar/sql-bars-window [:us :d] "BONGO" false
                      (t/instant "2005-01-01T00:00:00Z")
                      (t/instant "2010-03-01T20:00:00Z"))
;"SELECT * from us_d WHERE asset = 'BONGO' 
; and date >= '2005-01-01T00:00:00Z' 
; and date <= '2010-03-01T20:00:00Z' 
; order by date"

(ibar/sql-bars-since [:us :d] "BONGO" false
                     (t/instant "2005-01-01T00:00:00Z"))
; "SELECT * from us_d WHERE asset = 'BONGO' and date >= '2005-01-01T00:00:00Z' order by date"

(ibar/sql-bars-until [:us :d] "BONGO" false
                     (t/instant "2010-01-01T00:00:00Z"))
; "SELECT * from us_d WHERE asset = 'BONGO' and date <= '2010-01-01T00:00:00Z' order by date"

(ibar/get-bars
 (:conn bardb)
 {:asset "BONGO"}
 {:calendar [:us :d]
       ;:start (t/instant "2005-01-06T00:00:00Z")
  :end (t/instant "2005-01-14T22:00:00Z")
  :n 3})

(query (:conn bardb) "SELECT schema_name FROM information_schema.schemata;")
(query (:conn bardb) "PRAGMA show_tables;")

(query (:conn bardb) "SELECT  column_name, data_type, is_nullable
              FROM information_schema.columns
              WHERE table_name = 'crypto_d'
              ORDER BY ordinal_position;")

(query (:conn bardb) "CREATE TABLE prices AS
              SELECT * FROM us_d
              ORDER BY asset, date;")

(query (:conn bardb) "SELECT * FROM prices;")

(query (:conn bardb) "SELECT *
              FROM information_schema.columns
              WHERE table_name = 'prices'
              ORDER BY ordinal_position;")

(query (:conn bardb) "SELECT column_name, data_type, is_nullable
              FROM information_schema.columns
              WHERE table_name = 'prices_sorted'
              ORDER BY ordinal_position;")

(query (:conn bardb) "PRAGMA table_info('crypto_d');")

(m/? (generate-bars bardb
                    {:asset "A"
                     :calendar [:us :d]
                     :start (t/instant "2005-01-01T00:00:00Z")
                     :end  (t/instant "2010-03-01T20:00:00Z")}))

(m/? (generate-bars bardb
                    {:asset "BBB"
                     :calendar [:us :d]
                     :start (t/instant "2005-01-01T00:00:00Z")
                     :end  (t/instant "2010-03-01T20:00:00Z")}))

(m/? (b/summary bardb {:calendar [:us :d]}))

(m/? (b/get-bars bardb  {:asset "BBB" :calendar [:us :d]} {}))

(query (:conn bardb) "SELECT *
                      FROM us_d
                      WHERE asset = ANY (['BONGO', 'BBB']);")

(query (:conn bardb) "SELECT *
                      FROM us_d
                      WHERE asset IN ('BONGO', 'BBB');")

(ibar/sql-where-asset ["BONGO" "BBB"] true)

(m/? (b/get-bars bardb  {:asset ["BONGO" "BBB"] :calendar [:us :d]} {}))

(m/? (b/get-bars bardb  {:asset ["BONGO" "BBB"] :calendar [:us :d]}
                 {:start (t/instant "2010-02-22T22:00:00Z")}))

(str "WHERE asset IN ("
     (clojure.string/join "," (repeat (count assets) "?"))
     ")")