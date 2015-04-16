(ns events.core
  (:require [events.processor :refer [make-processor]]
            [clojure.data.json :as json]
            [clojure.string :refer [join]]))

(def progress-file "progress.out")

(defn- read-json [reader]
  (let [lines (line-seq reader)]
    (map json/read-str lines)))

(defn- save-state [state]
  (fn []
    (when-not (:complete (meta @state))
      (with-open [out (clojure.java.io/writer progress-file)]
        (binding [*print-meta* true
                  *out* out]
          (pr @state))))))

(defn- hydrate-state []
  (if (.exists (clojure.java.io/file progress-file))
    (read-string (slurp progress-file))
    (with-meta {}
      {:dups #{}
       :n 0})))

(defn serialize-results [state]
  (with-open [out (clojure.java.io/writer "result.out")]
    (doseq [[user-id data] @state]
      (.write out (str (join (interpose ","
                                        (cons user-id
                                              (map (fn [[k v]] (str k "=" v)) data))))
                       \newline)))))

(defn process-events [event-stream state]
  (let [processor (make-processor state)]
    (doseq [event event-stream]
      (processor event))))

(defn -main [file]
  (let [state (atom (hydrate-state))]
    (.addShutdownHook (Runtime/getRuntime)
                      (Thread. (save-state state)))
    (with-open [in (clojure.java.io/reader file)]
      (process-events (drop (:n (meta @state))
                            (read-json in))
                      state))
    (serialize-results state)
    (swap! state with-meta {:complete true})
    (clojure.java.io/delete-file "progress.out" true)))
