(ns events.processor)

(defn- get-timestamp [user-state]
  (get (meta user-state) :timestamp 0))

(defn- duplicated-event? [state event]
  (get-in (meta @state) [:dups (event "id")]))

(defn- add-to-dups [state event]
  (update-in (meta state)
             [:dups]
             conj (event "id")))

(defn- timestamped-merge [timestamp state data]
  (with-meta (merge state data) ; Add timestamp to user data
    {:timestamp timestamp}))

(defn- merge-user-state [state event]
  (let [user-id (event "user_id")
        timestamp (event "timestamp")
        data (event "data")]
    (when (> timestamp (get-timestamp (state user-id)))
      (update-in state
                 [user-id]
                 (partial timestamped-merge timestamp)
                 data))))

(defn- merge-state [state event]
  (with-meta (or (merge-user-state state event)
                 state)
    (update-in (add-to-dups state event)
               [:n]
               inc)))

(defn- process [event state]
  (when-not (duplicated-event? state event)
    (swap! state merge-state event)))

(defn make-processor [state]
  (fn [event]
    (process event state)))
