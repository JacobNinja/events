(ns events.processor)

(defn- timestamped-merge [timestamp state data]
  (with-meta (merge state data)
    {:timestamp timestamp}))

(defn- merge-state [state user_id data timestamp]
  (if (> timestamp (get (meta (state user_id)) :timestamp 0))
    (update-in state [user_id] (partial timestamped-merge timestamp) data)
    state))

(defn process [{:keys [user_id data timestamp]} state]
  (swap! state merge-state user_id data timestamp))

(defn make-processor [state]
  (fn [event]
    (process event state)))
