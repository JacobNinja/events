(ns events.processor-test
  (:require [clojure.test :refer :all]
            [events.processor :refer :all]))

(def event
  {
   :id "c52543d8-da03-11e4-8e29-c5dc2fe5941b",
   :type "attributes"
   :user_id "2352"
   :data {"name" "Bill", "email" "bill@gmail.com"}
   :timestamp 1428067050
   })

(defn- new-event [event-data]
  (merge event
         {:id (str (java.util.UUID/randomUUID))}
         event-data))

(deftest process-test
  (testing "stores event data by user_id"
    (let [state (atom {})
          process-event (make-processor state)]
      (process-event event)
      (is (= @state
             {"2352" {"name" "Bill", "email" "bill@gmail.com"}}))))

  (testing "stores data with latest timestamp"
    (let [state (atom {})
          process-event (make-processor state)
          early-event (new-event {:data {"name" "Graham"}
                                  :timestamp (dec (:timestamp event))})
          late-event (new-event {:data {"name" "Bob"}
                                 :timestamp (inc (:timestamp event))})]
      (process-event event)
      (is (= @state
             {"2352" (:data event)}))
      (process-event early-event)
      (is (= @state
             {"2352" (:data event)}))
      (process-event late-event)
      (is (= @state
             {"2352" (merge (:data event)
                            (:data late-event))}))))

  (testing "de-dupes by id"
    (let [state (atom {})
          process-event (make-processor state)
          duped-event (merge event {:data {"foo" "bar"}
                                    :timestamp (inc (:timestamp event))})]
      (process-event event)
      (process-event duped-event)
      (is (= @state
             {"2352" (:data event)}))))

  )
