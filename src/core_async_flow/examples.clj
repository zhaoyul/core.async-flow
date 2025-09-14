(ns core-async-flow.examples
  (:require [clojure.core.async :as async :refer [go go-loop <! >! chan close! timeout]]))

;; Reusable example functions for notebooks

(defn create-producer [ch name items delay-ms]
  "Creates a producer that puts items on a channel with specified delay"
  (go
    (doseq [item items]
      (<! (timeout delay-ms))
      (>! ch {:producer name :item item :timestamp (System/currentTimeMillis)}))
    (println (str "Producer " name " finished"))))

(defn create-consumer [ch name]
  "Creates a consumer that takes items from a channel"
  (go-loop [consumed []]
    (if-let [item (<! ch)]
      (do 
        (println (str "Consumer " name " got: " (:item item) " from " (:producer item)))
        (recur (conj consumed item)))
      consumed)))

(defn rate-limited-producer [output-ch rate-per-second items]
  "Producer that respects a rate limit"
  (let [rate-ch (chan)]
    (go-loop []
      (<! (timeout (/ 1000 rate-per-second)))
      (>! rate-ch :tick)
      (recur))
    
    (go 
      (doseq [item items]
        (<! rate-ch) ; Wait for rate limiter
        (>! output-ch item))
      (close! output-ch))))

(defn pipeline-transform [input-ch xform parallelism]
  "Creates a transformation pipeline"
  (let [output-ch (chan)]
    (async/pipeline parallelism output-ch xform input-ch)
    output-ch))

(defn monitored-channel [ch name]
  "Wraps a channel with monitoring"
  (let [monitored-ch (chan)
        stats (atom {:puts 0 :takes 0 :name name})]
    
    ;; Monitor puts
    (go-loop []
      (if-let [item (<! ch)]
        (do 
          (swap! stats update :takes inc)
          (>! monitored-ch item)
          (recur))
        (close! monitored-ch)))
    
    {:channel monitored-ch :stats stats}))

(defn circuit-breaker-channel [ch failure-threshold reset-timeout-ms]
  "Wraps a channel with circuit breaker logic"
  (let [state (atom {:status :closed :failures 0 :last-failure nil})
        wrapped-ch (chan)]
    
    (go-loop []
      (if-let [item (<! ch)]
        (case (:status @state)
          :closed 
          (>! wrapped-ch item)
          
          :open
          (when (and (:last-failure @state)
                     (> (System/currentTimeMillis) 
                        (+ (:last-failure @state) reset-timeout-ms)))
            (swap! state assoc :status :half-open))
          
          :half-open
          (>! wrapped-ch item))
        (close! wrapped-ch)))
    
    {:channel wrapped-ch :state state}))

(defn batch-processor [input-ch batch-size timeout-ms]
  "Batches items from input channel"
  (let [output-ch (chan)]
    (go-loop [batch [] last-emit (System/currentTimeMillis)]
      (let [timeout-ch (timeout timeout-ms)
            [item port] (async/alts! [input-ch timeout-ch])]
        (cond 
          (= port input-ch)
          (if item
            (let [new-batch (conj batch item)]
              (if (>= (count new-batch) batch-size)
                (do 
                  (>! output-ch new-batch)
                  (recur [] (System/currentTimeMillis)))
                (recur new-batch last-emit)))
            (when (seq batch)
              (>! output-ch batch)
              (close! output-ch)))
          
          (= port timeout-ch)
          (if (seq batch)
            (do 
              (>! output-ch batch)
              (recur [] (System/currentTimeMillis)))
            (recur batch last-emit)))))
    output-ch))

(defn merge-channels [channels]
  "Merges multiple channels into one"
  (let [output-ch (chan)]
    (go 
      (loop [remaining-chs (set channels)]
        (if (seq remaining-chs)
          (let [[item port] (async/alts! (vec remaining-chs))]
            (if item
              (do 
                (>! output-ch item)
                (recur remaining-chs))
              (recur (disj remaining-chs port))))
          (close! output-ch))))
    output-ch))