(ns events.core
  (:require [events.processor :refer [make-processor]]
            [clojure.data.json :as json]))

(defn- read-json [file]
  (let [lines (line-seq (clojure.java.io/reader file))]
    (map json/read-str lines)))

(defn process-events [event-stream]
  (let [state (atom {})
        processor (make-processor state)]
    (doseq [event event-stream]
      (processor event))))

(defn -main [file]
  (process-events (read-json file)))
