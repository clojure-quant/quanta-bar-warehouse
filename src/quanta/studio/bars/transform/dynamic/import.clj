(ns quanta.studio.bars.transform.dynamic.import
  (:require
   [taoensso.timbre :as timbre :refer [debug info warn error]]
   [tick.core :as t]
   [de.otto.nom.core :as nom]
   [ta.db.bars.protocol :as b]
   [quanta.studio.bars.transform.dynamic.overview-db :as overview]
   [quanta.studio.bars.transform.logger :as logger]))

(defn- import-tasks-map [req-window db-window]
  {:db-empty (when (not db-window)
               {:type :db-empty
                :start (:start req-window)
                :end (:end req-window)
                :db {:start (:start req-window)
                     :end (:end req-window)}})
   :missing-prior (when  (and db-window (t/> (:start db-window) (:start req-window)))
                    {:type :missing-prior
                     :start (:start req-window)
                     :end (:start db-window)
                     :db {:start (:start req-window)}})
   :missing-after (when (and db-window (t/< (:end db-window) (:end req-window)))
                    {:type :missing-after
                     :start (:end db-window)
                     :end (:end req-window)
                     :db {:end (:end req-window)}})})

(defn import-tasks [req-window db-window]
  (->> (import-tasks-map req-window db-window)
       (vals)
       (remove nil?)))

(defn import-needed? [tasks]
  (not (empty? tasks)))

(defn get-bars-safe [state opts task]
  (try
    (let [bar-ds (b/get-bars (:importer state) opts task)]
      (if bar-ds
        bar-ds
        (nom/fail ::get-bars-safe {:message "import-provider has returned nil."
                                   :opts opts
                                   :task task})))
    (catch AssertionError ex
      (nom/fail ::get-bars-safe {:message "import-provider get-bars has thrown an assertion error"
                                 :opts opts
                                 :task task}))
    (catch Exception ex
      (error "get-bars-safe " (select-keys opts [:task-id :asset :calendar :import])
             "task: " task "failed: reason import-provider get-bars exception: " ex)
      (nom/fail ::get-bars-safe {:message "import-provider get-bars has thrown an exception"
                                 :opts opts
                                 :task task
                                 :ex (ex-cause ex)}))))

(defn append-bars-safe [state opts task bar-ds]
  (try
    (when bar-ds
      (debug "dynamically received ds-bars! appending to db...")
      (b/append-bars (:bar-db state) opts bar-ds)
      (overview/update-range (:overview-db state) opts (:db task)))
    (catch AssertionError ex
      (error "append-bars " (select-keys opts [:task-id :asset :calendar :import]) " assert error: " ex)
      nil)
    (catch Exception ex
      (error "append-bars  " (select-keys opts [:task-id :asset :calendar :import]) " exception : " ex)
      nil)))

(defn run-import-task [state opts task]
  (let [bar-ds (get-bars-safe state opts task)]
    (cond
      (nil? bar-ds)
      (error "run-import-task " (select-keys opts [:task-id :asset :calendar :import]) "failed: imported bar-ds is nil.")

      (nom/anomaly? bar-ds)
      (error "run-import-task " (select-keys opts [:task-id :asset :calendar :import]) " failed. anomaly: " bar-ds)

      :else
      (append-bars-safe state opts task bar-ds))))

(defn run-import-tasks [state opts tasks]
  (doall (map #(run-import-task state opts %) tasks)))

(defn tasks-for-request [state {:keys [asset calendar import] :as opts} req-window]
  (if import
    (let [db-window (overview/available-range (:overview-db state) opts)
          tasks (import-tasks req-window db-window)]
      tasks)
    (do (warn "no import defined for asset: " asset " calendar: " calendar)
        '())))

(defn import-on-demand [state {:keys [asset calendar] :as opts} req-window]
  (info "import-on-demand " (select-keys opts [:task-id :asset :calendar :import]) req-window)
  (let [tasks (tasks-for-request state opts req-window)]
    (when (import-needed? tasks)
      (info "running import-tasks: " tasks)
      (logger/import-on-demand opts req-window tasks)
      (run-import-tasks state opts tasks))))
