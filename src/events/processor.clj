(ns events.processor)

(defn- get-timestamp [user-state]
  (get (meta user-state) :timestamp 0))

(defn- duplicated-event? [state event-data]
  (get-in (meta @state) [:dups (:id event-data)]))

(defn- timestamped-merge [timestamp state data]
  (with-meta (merge state data) ; Add timestamp to user data
    {:timestamp timestamp}))

(defn- merge-user-state [state {:keys [user-id data timestamp]}]
  (when (> timestamp (get-timestamp (state user-id)))
    (update-in state
               [user-id]
               (partial timestamped-merge timestamp)
               data)))

(defn- merge-state [state event-data]
  (with-meta (or (merge-user-state state event-data)
                 state)
    (update-in (meta state) [:dups] conj (:id event-data)))) ; Add event id to dups set

(defn- process [event-data state]
  (when-not (duplicated-event? state event-data)
    (swap! state merge-state event-data)))

(defn make-processor
  ([state] (make-processor state #{})) ; Dups default to empty set
  ([state dups]
   (swap! state with-meta {:dups dups}) ; Associate dups meta data
   (fn [event]
     (process event state))))
