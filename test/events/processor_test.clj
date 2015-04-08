(ns events.processor-test
  (:require [clojure.test :refer :all]
            [events.processor :refer :all]))

(def event
  {
   ;:id "c52543d8-da03-11e4-8e29-c5dc2fe5941b",
   :type "attributes"
   :user_id "2352"
   :data {"name" "Bill", "email" "bill@gmail.com"}
   :timestamp 1428067050
   })

(deftest process-test
  (let [state (atom {})
        process-event (make-processor state)]
    (testing "stores event data by user_id"
      (process-event event)
      (is (= @state
             {"2352" {"name" "Bill", "email" "bill@gmail.com"}})))
    (testing "stores data with latest timestamp"
      (let [early-event (merge event {:data {"name" "Graham"}
                                      :timestamp (dec (:timestamp event))})
            late-event (merge event {:data {"name" "Bob"}
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
    ))
