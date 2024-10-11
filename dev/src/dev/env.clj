(ns dev.env
  (:require
   [clojure.edn :as edn]
   ;; asset db
   [ta.db.asset.db :as asset-db]
   ; bardb
   [ta.db.bars.nippy :refer [start-bardb-nippy]]
   ;; imports
   [ta.import.provider.bybit.ds :refer [create-import-bybit]]
   [ta.import.provider.parallel :refer [create-import-bybit-parallel]]
   [ta.import.provider.kibot.ds :refer [create-import-kibot]]
   [ta.import.provider.kibot-http.ds :refer [create-import-kibot-http]]
   [ta.import.provider.eodhd.ds :refer [create-import-eodhd]]
   [ta.import.provider.alphavantage.ds :refer [create-import-alphavantage]]
   ;; bar-engine
   [quanta.studio.bars.engine :refer [start-bar-engine]]
   ; transform
   [quanta.studio.bars.transform.compress :refer [start-transform-compress]]
   [quanta.studio.bars.transform.shuffle :refer [start-transform-shuffle]]))

(def bardb-nippy
  (start-bardb-nippy ".data/nippy/"))

(def secrets 
  (-> "/home/florian/repo/myLinux/myvault/goldly/quanta.edn"
      (slurp) 
      (edn/read-string)))

(def bar-engine
  (start-bar-engine
   {:bardb {:nippy bardb-nippy}
    :import {:kibot (create-import-kibot (:kibot secrets))
             :kibot-http (create-import-kibot-http (:kibot secrets))
             :eodhd (create-import-eodhd (:eodhd secrets))
             :alphavantage (create-import-alphavantage (:alphavantage secrets))
             :bybit (create-import-bybit)
             :bybit-parallel (create-import-bybit-parallel)}
    :transform {:compress (start-transform-compress
                           {; we just request daily and minute bars, the rest gets calculated.
                            :Y :d
                            :M :d
                            :W :d
                            ;:d :d ; use daily from source.
                            :h :m
                            :m30 :m
                            :m15 :m
                            :m5 :m
                            ;:m :m ; use minute from source.
                            })
                :shuffle (start-transform-shuffle)}}))



(def assets
  [; kibot
   {:name "EURUSD" :symbol "EUR/USD" :kibot "EURUSD" :category :fx}
   {:name "Microsoft" :symbol "MSFT" :kibot "MSFT" :category :equity}])


(doall
 (map asset-db/add assets))


