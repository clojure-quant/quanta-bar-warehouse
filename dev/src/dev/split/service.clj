(ns dev.split.service
  (:require
   [tick.core :as t]
   [missionary.core :as m]
   [tech.v3.dataset :as tds]
   [tablecloth.api :as tc]
   [quanta.calendar.window :as w]
   [quanta.bar.protocol :as b]
   [quanta.bar.split.service :refer [save-splits delete-splits get-splits]]
   [dev.env :refer [ss bardbduck]]))

(def split-ds
  (-> {:date [(t/instant "1987-09-21T00:00:00Z")
              (t/instant "1990-04-16T00:00:00Z")
              (t/instant "1991-06-27T00:00:00Z")
              (t/instant "1992-06-15T00:00:00Z")
              (t/instant "1994-05-23T00:00:00Z")
              (t/instant "1996-12-09T00:00:00Z")
              (t/instant "1998-02-23T00:00:00Z")
              (t/instant "1999-03-29T00:00:00Z")
              (t/instant "2003-02-18T00:00:00Z")]
       :factor [2.0 2.0 1.5 1.5 2.0 2.0 2.0 2.0 2.0]}
      (tc/dataset)
      (tc/add-column :asset "MSFT")))

split-ds

(m/? (save-splits ss split-ds))

(m/? (get-splits ss "MSFT"))

(m/? (delete-splits ss "MSFT"))

(m/? (get-splits ss "MSFT"))

(m/? (b/get-bars (:barsource ss)
                 {:asset "MSFT"
                  :calendar [:us :d]}
                 {:start (t/instant "2005-01-01T00:00:00Z")
                  :end (t/instant "2010-03-01T20:00:00Z")}))

;; note the last splits was in 2003, so the factor is always 1.

(m/? (b/get-bars (:barsource ss)
                 {:asset "MSFT"
                  :calendar [:us :d]}
                 {:start (t/instant "2002-01-01T00:00:00Z")
                  :end (t/instant "2003-03-01T20:00:00Z")}))

;| :asset |                :date |  :open |  :high |   :low | :close | :volume | :ticks | :factor |
;|--------|----------------------|-------:|-------:|-------:|-------:|--------:|-------:|--------:|
;|   MSFT | 2003-01-01T00:00:00Z |  9.655 |  9.655 |  9.655 |  9.655 |     0.0 |      0 |     2.0 |
;|   MSFT | 2003-02-01T00:00:00Z |  9.670 |  9.670 |  9.670 |  9.670 |     0.0 |      0 |     2.0 |
;|   MSFT | 2003-03-01T00:00:00Z | 19.760 | 19.760 | 19.760 | 19.760 |     0.0 |      0 |     1.0 |

; last split was in 2003-02-18, so 2003-01 is not adjusted, and 2002-01 has the adjustment

;; TEST WHEN there are no splits for an asset

(m/? (get-splits ss "AAPL"))

(m/? (b/get-bars (:barsource ss)
                 {:asset "AAPL"
                  :calendar [:us :d]}
                 {:start (t/instant "1999-01-01T00:00:00Z")
                  :end (t/instant "2003-03-01T20:00:00Z")}))

;; it works .. the :factor is alwas 1.0

;; test for stock that does not exist 

(m/? (b/get-bars bardbduck
                 {:asset "XXX-BAD"
                  :calendar [:us :d]}
                 {:start (t/instant "1999-01-01T00:00:00Z")
                  :end (t/instant "2003-03-01T20:00:00Z")}))

;; _unnamed [0 0]
(m/? (get-splits ss "XXX-BAD"))
;; _unnamed [0 0]