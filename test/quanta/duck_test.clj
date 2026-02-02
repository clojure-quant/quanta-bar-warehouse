(ns quanta.duck-test
  (:require
   [clojure.test :refer [deftest is]]
   [tick.core :as t]
   [tablecloth.api :as tc]
   [missionary.core :as m]
   [babashka.fs :as fs]
   [quanta.calendar.window :refer [date-range->window]]
   [quanta.bar.protocol :as b]
   [quanta.bar.db.duck :as duck]
   [quanta.bar.db.duck.warehouse :as wh]))

;; Test if duckdb get/append works

(def db-name (str "/tmp/" (gensym "duck-demo") ".ddb"))

(fs/delete-if-exists db-name)
(println "creating duckdb : " db-name)

(def db-duck (duck/start-bardb-duck db-name))

(def ds-write
  (tc/dataset [{:date (-> "1999-12-31T00:00:00Z" t/instant) :asset "QQQ"
                :open 1.0 :high 1.0 :low 1.0 :close 1.0 :volume 1.0 :ticks 0}
               {:date (-> "2000-12-31T00:00:00Z" t/instant) :asset "QQQ"
                :open 1.0 :high 1.0 :low 1.0 :close 1.0 :volume 1.0 :ticks 0}]))

(m/? (b/append-bars db-duck {:asset "QQQ"
                             :calendar [:us :d]} ds-write))

(def window (date-range->window
             [:us :d]
             {:start (-> "1999-02-01T20:00:00Z" t/instant)
              :end (-> "2001-03-01T20:00:00Z" t/instant)}))
window

; just get the window
(def ds-read
  (m/? (b/get-bars db-duck
                   {:asset "QQQ"
                    :calendar [:us :d]}
                   window)))

(deftest reload-ok
  (is (= ds-write ds-read) "reloaded dataset must be equal"))

(def summary-should
  (tc/dataset [{:asset "QQQ"
                :start (-> "1999-12-31T00:00:00Z" t/instant)
                :end (-> "2000-12-31T00:00:00Z" t/instant)
                :count 2}]))

(def summary-read
  (wh/warehouse-summary db-duck [:us :d]))

(deftest summary-ok
  (is (= summary-should summary-read) "warehouse summary should match the series"))

; clean shutdown

(duck/stop-bardb-duck db-duck)
