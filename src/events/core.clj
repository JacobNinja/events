(ns events.core
  (:require [events.processor :refer [make-processor]]
            [clojure.data.json :as json]))

(defn- keywordize [m]
  (into {}
        (for [[k v] m]
          [(keyword k) v])))

(defn- read-json [file]
  (let [lines (line-seq (clojure.java.io/reader file))]
         (map #(keywordize (json/read-str %))
              lines)))

(defn process-events [event-stream]
  (let [state (atom {})
        processor (make-processor state)]
    (doseq [event event-stream]
      (processor event))))

(defn -main [file]
  (process-events (read-json file)))
