(ns quanta.bar.transform.append-only
  (:require
    [taoensso.timbre :as timbre :refer [debug info warn error]]
    [de.otto.nom.core :as nom]
    [missionary.core :as m]
    [tablecloth.api :as tc]
    [tick.core :as t]
    [ta.calendar.validate :as cal]
    [ta.db.bars.protocol :refer [barsource] :as b]
    [quanta.bar.transform.helper :refer [get-source-interval]]))

(defn load-stored-bars [opts window]
  (let [engine (:engine opts)
        opts-clean (dissoc opts :engine)]
    (b/get-bars engine opts-clean window)))

(defn get-first-dt [ds]
  (->> (tc/first ds) :date first))

(defn get-last-dt [ds]
  (->> (tc/last ds) :date first))

(defn missing-bars-windows
  "possible scenarios:
  1) requested window is equal to ds window                         => no append
  2) requested window starts earlier but ends inside ds window      => append pre window
  3) requested window starts inside ds window but ends later        => append post window
  4) requested window is included in ds window                      => no append
  5) requested window starts earlier and ands later than ds window  => append pre + post window
  6) ds is empty                                                    => append whole window"
  [ds calendar window]
  (if (tc/empty-ds? ds)
    [window]
    (let [first-dt (get-first-dt ds)
          last-dt (get-last-dt ds)
          ; TODO: align bar time of request window
          pre-window (if (t/< (:start window) first-dt)
                       {:start (:start window)
                        :end first-dt})  ; TODO -1 bar
          post-window (if (t/> (:end window) last-dt)
                        {:start last-dt ; TODO +1 bar
                         :end (:end window)})]
      (debug "calc missing bar windows"
             {:first-dt first-dt
              :last-dt last-dt
              :pre-window pre-window
              :post-window post-window})
      (filter #(not (nil? %)) [pre-window post-window]))))

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
                [pre-window post-window] (missing-bars-windows ds-higher calendar-source window)
                ; TODO append windows
                ;_ (warn "append-only [" interval-source "=> " interval "] opts: " (select-keys opts-source [:task-id :asset :calendar :import]))
                ]
            (cond
              (not ds-higher)
              (nom/fail ::append-only {:message "cannot append dataset, ds-higher is nil"
                                    :opts opts
                                    :range window})

              (nom/anomaly? ds-higher)
              ds-higher

              :else
              ds-higher
              ))
          (do
            (warn "no append for: " interval " - forwarding request to bar-engine ")
            (let [stored-ds (m/? (load-stored-bars opts window))
                  [pre-window post-window] (missing-bars-windows stored-ds calendar window)]
              ;; TODO: append windows
              stored-ds
              ))
          )
        )
    )))


(defn start-transform-append-only [interval-config]
  (assert (not (nil? interval-config)) "interval-config needs to be a map. currently it is nil.")
  (assert (map? interval-config) "append-only-provider interval-config must be a map")
  (transform-append-only. interval-config))
