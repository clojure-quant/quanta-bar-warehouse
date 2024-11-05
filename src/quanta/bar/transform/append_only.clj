(ns quanta.bar.transform.append-only
  (:require
   [taoensso.timbre :as timbre :refer [debug info warn error]]
   [de.otto.nom.core :as nom]
   [missionary.core :as m]
   [tablecloth.api :as tc]
   [tick.core :as t]
   [ta.calendar.validate :as cal]
   [ta.db.bars.protocol :refer [barsource] :as b]
   [quanta.calendar.core :refer [current-close next-close prior-close]]
   [quanta.bar.transform.helper :refer [get-last-dt get-source-interval load-stored-bars]])
  (:import (java.io FileNotFoundException)))

(defn missing-bars-window
  "possible scenarios:
  1) requested window is equal to ds window                         => no append
  2) requested window starts earlier but ends inside ds window      => no append
  3) requested window starts inside ds window but ends later        => append post window
  4) requested window is included in ds window                      => no append
  5) requested window starts earlier and ands later than ds window  => append post window only
  6) ds is empty                                                    => append whole window

  NOTE: expects window with bar close time!
  returns a window or nil if no bar is missing
  "
  [ds calendar window]
  (if (tc/empty-ds? ds)
    window
    (let [last-dt (get-last-dt ds)
          post-window (when (t/> (:end window) last-dt)
                        {:start (next-close calendar last-dt)
                         :end (:end window)})]
      post-window)))

(defn aligned-window
  "align source window to target timeframe.
   prepend missing bars before start
   truncate bars after end if the target bar cannot be finished with the available source bars"
  [calendar calendar-source {:keys [start end] :as window}]
  (when window
    {:start (->> (prior-close calendar start)               ; needs to be prior close, else we only have the last source bar for the first bar
                 (next-close calendar-source))
     :end (current-close calendar end)}))

(defn compress-bars [stored-ds opts window]
  (let [uncompressed-window (missing-bars-window stored-ds (:calendar opts) window)
        compress-window (if uncompressed-window
                          uncompressed-window
                          window)
        engine (:engine opts)
        opts-clean (select-keys (assoc opts :transform :compress)
                                [:asset :calendar :bardb :to :transform])]
    (info "append-only - compress-bars" opts-clean "compress window:" compress-window "full window:" window)
    (b/get-bars engine opts-clean compress-window)))

(defn import-bars [opts window]
  (assert (contains? opts :import) ":import not set. need importer for missing bars")
  (let [engine (:engine opts)
        opts-clean (select-keys opts [:asset :calendar :bardb :import])]
    (info "append-only - import-bars" opts-clean window)
    (b/get-bars engine opts-clean window)))

(defn append-bars [opts bar-ds]
  (assert (contains? opts :to) ":to not set. need target bardb for appending")
  (let [{:keys [engine to asset calendar]} opts
        opts-clean {:asset asset
                    :calendar calendar
                    :bardb to
                    :write-mode :append}]
    (info "append-only - append-bars" opts-clean)
    (b/append-bars engine opts-clean bar-ds)))

(defrecord transform-append-only [interval-config]
  barsource
  (get-bars [this opts window]
    (m/sp
     (info "get-bars " (select-keys opts [:task-id :asset :calendar :import])
           " window: " (select-keys window [:start :end]))
     (let [calendar  (:calendar opts)
           market (cal/exchange calendar)
           interval (cal/interval calendar)
           interval-source (get-source-interval (:interval-config this) interval)]
       (if interval-source
         (let [calendar-source [market interval-source]
               opts-source (assoc opts :calendar calendar-source)
               aligned-source-window (aligned-window calendar calendar-source window)
               ds-higher (try
                           (m/? (load-stored-bars opts-source aligned-source-window))
                           (catch FileNotFoundException ex
                             (tc/dataset [])))
               _ (warn "append-only [" interval-source "=> " interval "] opts: " (select-keys opts-source [:task-id :asset :calendar :import]))]
           (cond
             (not ds-higher)
             (nom/fail ::append-only {:message "cannot append dataset, ds-higher is nil"
                                      :opts opts
                                      :range window})

             (nom/anomaly? ds-higher)
             ds-higher

             :else
             (try
               (assert (t/<= (:start window) (:end window)) (str "invalid window: " window))
               ;; import
               (let [import-window (missing-bars-window ds-higher calendar-source aligned-source-window)]
                 (when import-window
                   (let [imported-source-ds (m/? (import-bars opts-source import-window))
                         _ (append-bars opts-source imported-source-ds)])))
               ;; compress
               (let [ds (try
                          (m/? (load-stored-bars opts aligned-source-window))
                          (catch FileNotFoundException ex
                            (tc/dataset [])))
                     compressed-ds (m/? (compress-bars ds opts aligned-source-window))
                     _ (append-bars opts compressed-ds)])
               ;; load
               (try
                 (m/? (load-stored-bars opts window))
                 (catch FileNotFoundException ex
                   (tc/dataset [])))
               (catch AssertionError ex
                 (error "append-only: interval:" interval opts aligned-source-window " exception: " ex))
               (catch Exception ex
                 (error "append-only: interval:" interval opts aligned-source-window " exception: " ex)))))
         (do
           (info "append-only for" interval "- forwarding request to bar-engine")
           (try
             (let [stored-ds (try
                               (m/? (load-stored-bars opts window))
                               (catch FileNotFoundException ex
                                 (tc/dataset [])))
                   import-window (missing-bars-window stored-ds calendar window)]
               (when import-window
                 (let [imported-ds (m/? (import-bars opts import-window))]
                   (append-bars opts imported-ds)))
               (try
                 (m/? (load-stored-bars opts window))
                 (catch FileNotFoundException ex
                   (tc/dataset []))))
             (catch AssertionError ex
               (error "append-only: assertion:" interval window opts " exception: " ex))
             (catch Exception ex
               (error "append-only: interval:" interval window opts " exception: " ex)))))))))

(defn start-transform-append-only [interval-config]
  (assert (not (nil? interval-config)) "interval-config needs to be a map. currently it is nil.")
  (assert (map? interval-config) "append-only-provider interval-config must be a map")
  (transform-append-only. interval-config))
