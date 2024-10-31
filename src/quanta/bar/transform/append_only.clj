(ns quanta.bar.transform.append-only
  (:require
   [taoensso.timbre :as timbre :refer [debug info warn error]]
   [de.otto.nom.core :as nom]
   [missionary.core :as m]
   [tablecloth.api :as tc]
   [tick.core :as t]
   [ta.calendar.validate :as cal]
   [ta.db.bars.protocol :refer [barsource] :as b]
   [quanta.calendar.core :refer [current-close next-close]]
   [quanta.bar.transform.helper :refer [get-source-interval]]))

(defn get-last-dt [ds]
  (->> (tc/last ds) :date first))

(defn load-stored-bars [opts window]
  (let [engine (:engine opts)
        opts-clean (select-keys opts [:asset :calendar :bardb])]
    (b/get-bars engine opts-clean window)))

(defn load-and-compress-bars [opts window]
  (let [engine (:engine opts)
        opts-clean (select-keys (assoc opts :transform :compress)
                                [:asset :calendar :bardb :to :transform])]
    (info "append-only - load-and-compress-bars" opts-clean window)
    (b/get-bars engine opts-clean window)))

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
    [window]
    (let [last-dt (get-last-dt ds)
          post-window (when (t/> (:end window) last-dt)
                        {:start (next-close calendar last-dt)
                         :end (:end window)})]
      post-window)))

(defn aligned-window
  "align window to other timeframe"
  [calendar {:keys [start end] :as window}]
  (when window
    (let [aligned-start-dt (current-close calendar start)
          aligned-end-dt (current-close calendar end)]
      (when (and (t/>= aligned-start-dt start) (t/<= aligned-start-dt end)
                 (t/>= aligned-end-dt start) (t/<= aligned-end-dt end))
        {:start aligned-end-dt
         :end aligned-end-dt}))))

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
               ds-higher (m/? (load-stored-bars opts-source window))
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
               (let [import-window (missing-bars-window ds-higher calendar-source window)
                     aligned-import-window (aligned-window calendar import-window)]
                  ; if aligned-import-window is nil, then no need to import and compress source bars
                 (when aligned-import-window
                   (let [imported-source-ds (m/? (import-bars opts-source import-window))]
                     (append-bars opts-source imported-source-ds)))
                 (m/? (load-and-compress-bars opts window)))
               (catch AssertionError ex
                 (error "append-only: interval:" interval opts window " exception: " ex))
               (catch Exception ex
                 (error "append-only: interval:" interval opts window " exception: " ex)))))
         (do
           (info "append-only for" interval "- forwarding request to bar-engine")
           (try
             (let [stored-ds (m/? (load-stored-bars opts window))
                   import-window (missing-bars-window stored-ds calendar window)]
               (when import-window
                 (let [imported-ds (m/? (import-bars opts import-window))]
                   (append-bars opts imported-ds)))
               (m/? (load-stored-bars opts window)))
             (catch AssertionError ex
               (error "append-only: assertion:" interval window opts " exception: " ex))
             (catch Exception ex
               (error "append-only: interval:" interval window opts " exception: " ex)))))))))

(defn start-transform-append-only [interval-config]
  (assert (not (nil? interval-config)) "interval-config needs to be a map. currently it is nil.")
  (assert (map? interval-config) "append-only-provider interval-config must be a map")
  (transform-append-only. interval-config))
