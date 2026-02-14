(ns ta.db.bars.random-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [missionary.core :as m]
   [tablecloth.api :as tc]
   [tick.core :as t]
   [quanta.bar.protocol :as b]
   [ta.db.bars.random :as r]))

(deftest random-price-series-is-stable-across-intervals
  (testing "Generating 2024 and 2025 separately matches generating 2024-2025 in one go"
    (let [seed 424242
          s2024 (r/random-price-series {:seed seed
                                        :start "2024-01-01"
                                        :end "2024-12-31"})
          s2025 (r/random-price-series {:seed seed
                                        :start "2025-01-01"
                                        :end "2025-12-31"})
          sall (r/random-price-series {:seed seed
                                       :start "2024-01-01"
                                       :end "2025-12-31"})]

      ;; sanity: inclusive date ranges (2024 is leap year)
      (is (= 366 (count s2024)))
      (is (= 365 (count s2025)))
      (is (= 731 (count sall)))

      ;; main requirement: interval-invariance
      (is (= sall (vec (concat s2024 s2025)))
          "[2024] + [2025] must equal [2024..2025] generated and sliced")

      ;; boundary continuity: daily return between -1% and +1%
      (let [p-last-2024 (-> s2024 last :pricer)
            p-first-2025 (-> s2025 first :pricer)
            ratio (/ p-first-2025 p-last-2024)]
        (is (<= 0.99 ratio 1.01)
            "The price change across 2024-12-31 -> 2025-01-01 must be within ±1%"))

      ;; reproducibility
      (is (= s2024 (r/random-price-series {:seed seed
                                           :start "2024-01-01"
                                           :end "2024-12-31"})))
      (is (= s2025 (r/random-price-series {:seed seed
                                           :start "2025-01-01"
                                           :end "2025-12-31"}))))))

(deftest random-bar-source-is-reproducible-per-asset-calendar
  (testing "Bars are deterministic per (asset,calendar) and stable across intervals"
    (let [src (r/start-random-bar-source {:seed 101})
          opts-qqq {:asset "QQQ" :calendar [:us :d]}
          opts-spy {:asset "SPY" :calendar [:us :d]}

          w2024 {:start (t/instant "2024-01-01T00:00:00Z")
                 :end (t/instant "2024-12-31T00:00:00Z")}
          w2025 {:start (t/instant "2025-01-01T00:00:00Z")
                 :end (t/instant "2025-12-31T00:00:00Z")}
          wAll {:start (t/instant "2024-01-01T00:00:00Z")
                :end (t/instant "2025-12-31T00:00:00Z")}

          ds-2024 (m/? (b/get-bars src opts-qqq w2024))
          ds-2025 (m/? (b/get-bars src opts-qqq w2025))
          ds-all (m/? (b/get-bars src opts-qqq wAll))

          ds-2024-again (m/? (b/get-bars src opts-qqq w2024))
          ds-spy-2024 (m/? (b/get-bars src opts-spy w2024))]

      (is (= 366 (tc/row-count ds-2024)))
      (is (= 365 (tc/row-count ds-2025)))
      (is (= 731 (tc/row-count ds-all)))

      ;; main requirement: interval-invariance
      (is (= ds-all (tc/concat ds-2024 ds-2025)))

      ;; reproducibility for same asset/calendar
      (is (= ds-2024 ds-2024-again))

      ;; different asset => different stream (via per-(asset,calendar) seed)
      (is (not= (-> ds-2024 :close first)
                (-> ds-spy-2024 :close first))
          "Different assets should (almost surely) yield different price streams"))))
