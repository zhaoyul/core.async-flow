(ns core-async-flow.utils
  (:require [clojure.core.async :as async :refer [go go-loop <! >! chan close! timeout]]))

;; Utility functions for async programming

(defn now [] (System/currentTimeMillis))

(defn sleep-async [ms]
  "Async sleep using timeout channel"
  (timeout ms))

(defn channel-seq [ch]
  "Converts a channel to a lazy sequence (use with caution - can block)"
  (lazy-seq
    (when-let [item (async/<!! ch)]
      (cons item (channel-seq ch)))))

(defn channel-into-coll [ch]
  "Collects all items from a channel into a vector"
  (go-loop [items []]
    (if-let [item (<! ch)]
      (recur (conj items item))
      items)))

(defn channel-with-timeout [ch timeout-ms default-value]
  "Takes from channel with timeout, returns default if timeout occurs"
  (go 
    (let [timeout-ch (timeout timeout-ms)
          [result port] (async/alts! [ch timeout-ch])]
      (if (= port timeout-ch)
        default-value
        result))))

(defn debounce-channel [input-ch delay-ms]
  "Debounces a channel - only emits after delay-ms of inactivity"
  (let [output-ch (chan)]
    (go-loop [last-item nil last-time nil]
      (let [timeout-ch (when last-time (timeout (max 0 (- (+ last-time delay-ms) (now)))))
            channels (if timeout-ch [input-ch timeout-ch] [input-ch])
            [item port] (async/alts! channels)]
        (cond 
          (= port input-ch)
          (if item
            (recur item (now))
            (do 
              (when last-item (>! output-ch last-item))
              (close! output-ch)))
          
          (= port timeout-ch)
          (do 
            (when last-item (>! output-ch last-item))
            (recur nil nil)))))
    output-ch))

(defn throttle-channel [input-ch min-interval-ms]
  "Throttles a channel - emits at most once per interval"
  (let [output-ch (chan)]
    (go-loop [last-emit 0]
      (if-let [item (<! input-ch)]
        (let [now-ms (now)
              time-since-last (- now-ms last-emit)]
          (if (>= time-since-last min-interval-ms)
            (do 
              (>! output-ch item)
              (recur now-ms))
            (do 
              (<! (timeout (- min-interval-ms time-since-last)))
              (>! output-ch item)
              (recur (now)))))
        (close! output-ch)))
    output-ch))

(defn distinct-channel [input-ch]
  "Only emits distinct consecutive values"
  (let [output-ch (chan)]
    (go-loop [last-value ::none]
      (if-let [item (<! input-ch)]
        (if (not= item last-value)
          (do 
            (>! output-ch item)
            (recur item))
          (recur last-value))
        (close! output-ch)))
    output-ch))

(defn sliding-window [input-ch window-size]
  "Creates sliding windows of items"
  (let [output-ch (chan)]
    (go-loop [window []]
      (if-let [item (<! input-ch)]
        (let [new-window (vec (take window-size (conj window item)))]
          (>! output-ch new-window)
          (recur new-window))
        (close! output-ch)))
    output-ch))

(defn error-channel []
  "Creates an error reporting channel"
  (let [error-ch (chan)]
    {:report-error (fn [error context]
                     (go (>! error-ch {:error error 
                                       :context context 
                                       :timestamp (now)})))
     :error-channel error-ch}))

(defn with-error-handling [operation error-reporter]
  "Wraps an operation with error handling"
  (go 
    (try 
      (<! (operation))
      (catch Exception e
        ((:report-error error-reporter) e {:operation operation})
        nil))))

(defn retry-channel [operation max-retries initial-delay-ms]
  "Creates a channel that retries an operation"
  (go-loop [attempt 0 delay initial-delay-ms]
    (if (< attempt max-retries)
      (let [result (<! (operation))]
        (if (instance? Throwable result)
          (do 
            (<! (timeout delay))
            (recur (inc attempt) (* delay 2)))
          result))
      {:error "Max retries exceeded" :attempts attempt})))

(defn health-check-channel [check-fn interval-ms]
  "Periodically runs health checks"
  (let [health-ch (chan)]
    (go-loop []
      (let [result (try 
                     (<! (check-fn))
                     (catch Exception e
                       {:status :error :error (.getMessage e)}))]
        (>! health-ch (assoc result :timestamp (now)))
        (<! (timeout interval-ms))
        (recur)))
    health-ch))

(defn metrics-collector []
  "Collects and aggregates metrics"
  (let [metrics-ch (chan)
        state (atom {})]
    
    (go-loop []
      (when-let [metric (<! metrics-ch)]
        (swap! state update (:key metric) (fnil + 0) (:value metric 1))
        (recur)))
    
    {:channel metrics-ch
     :get-metrics #(deref state)
     :reset-metrics #(reset! state {})}))

(defn logger-channel [log-level]
  "Creates a logging channel"
  (let [log-ch (chan)]
    (go-loop []
      (when-let [log-entry (<! log-ch)]
        (when (>= (get {:debug 0 :info 1 :warn 2 :error 3} (:level log-entry) 0)
                  (get {:debug 0 :info 1 :warn 2 :error 3} log-level 1))
          (println (format "[%s] %s: %s" 
                          (java.time.Instant/ofEpochMilli (:timestamp log-entry))
                          (name (:level log-entry))
                          (:message log-entry))))
        (recur)))
    log-ch))

(defmacro log-async [log-ch level message]
  `(go (>! ~log-ch {:level ~level 
                    :message ~message 
                    :timestamp (now)})))