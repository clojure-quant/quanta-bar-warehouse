(ns dev.env
  (:require
   [clojure.edn :as edn]
   ;; asset db
   [quanta.market.asset.db :as asset-db]
   ; bardb
   [ta.db.bars.nippy :refer [start-bardb-nippy]]
   ;; imports
   [quanta.market.barimport.bybit.import :refer [create-import-bybit]]
   [quanta.market.barimport.bybit.import-parallel :refer [create-import-bybit-parallel]]
   [quanta.market.barimport.kibot.api :refer [create-import-kibot]]
   [quanta.market.barimport.kibot.http :refer [create-import-kibot-http]]
   [quanta.market.asset.load :refer [add-lists-to-db]]
   ;[ta.import.provider.eodhd.ds :refer [create-import-eodhd]]
   ;[ta.import.provider.alphavantage.ds :refer [create-import-alphavantage]]
   ;; bar-engine
   [quanta.bar.engine :refer [start-bar-engine]]
   ; transform
   [quanta.bar.transform.compress :refer [start-transform-compress]]
   [quanta.bar.transform.shuffle :refer [start-transform-shuffle]]))

(def bardb-nippy
  (start-bardb-nippy ".data/nippy/"))

(def secrets
  (-> (str (System/getenv "MYVAULT") "/quanta.edn")
      (slurp)
      (edn/read-string)))

(def bar-engine
  (start-bar-engine
   {:bardb {:nippy bardb-nippy}
    :import {:kibot (create-import-kibot (:kibot secrets))
             :kibot-http (create-import-kibot-http (:kibot secrets))
             ;:eodhd (create-import-eodhd (:eodhd secrets))
             ;:alphavantage (create-import-alphavantage (:alphavantage secrets))
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
   {:name "USDJPY" :symbol "USD/JPY" :kibot "USDJPY" :category :fx}
   {:name "Microsoft" :symbol "MSFT" :kibot "MSFT" :category :equity}])

#_(doall
   (map asset-db/add assets))

(add-lists-to-db)

(asset-db/instrument-details "EUR/USD")



