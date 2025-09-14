;; # Common Async Patterns
;; This notebook demonstrates common patterns and best practices for async programming.

(ns notebooks.03-async-patterns
  (:require [nextjournal.clerk :as clerk]
            [clojure.core.async :as async :refer [go go-loop <! >! <!! >!! chan close! 
                                                  timeout alts! mult tap untap
                                                  pipe pipeline pipeline-blocking
                                                  merge mix admix unmix toggle
                                                  put! take!]]
            [clojure.core.async.impl.protocols :as protocols]))

;; ## Rate Limiting and Backpressure

;; ### Simple Rate Limiter

^{::clerk/visibility {:result :show}}
(defn rate-limiter [rate-per-second]
  "Creates a channel that allows at most `rate-per-second` operations per second"
  (let [rate-ch (chan)]
    (go-loop []
      (<! (timeout (/ 1000 rate-per-second)))
      (>! rate-ch :tick)
      (recur))
    rate-ch))

^{::clerk/visibility {:result :show}}
(let [limiter (rate-limiter 2) ; 2 operations per second
      results (atom [])]
  
  (go-loop [i 0]
    (when (< i 5)
      (<! limiter) ; Wait for rate limiter
      (swap! results conj {:operation i :timestamp (System/currentTimeMillis)})
      (recur (inc i))))
  
  (Thread/sleep 3000)
  
  ;; Calculate time differences to show rate limiting
  (let [timestamps (map :timestamp @results)
        differences (map - (rest timestamps) timestamps)]
    {:results @results
     :time-differences differences}))

;; ### Backpressure with Buffered Channels

^{::clerk/visibility {:result :show}}
(defn create-backpressure-demo [buffer-size processing-delay]
  (let [work-ch (chan buffer-size)
        results (atom {:produced 0 :processed [] :blocked 0})]
    
    ;; Fast producer
    (go 
      (doseq [i (range 20)]
        (let [start-time (System/currentTimeMillis)]
          (>! work-ch i)
          (let [end-time (System/currentTimeMillis)]
            (swap! results update :produced inc)
            (when (> (- end-time start-time) 10) ; If put took more than 10ms
              (swap! results update :blocked inc)))))
      (close! work-ch))
    
    ;; Slow consumer
    (go-loop []
      (if-let [work (<! work-ch)]
        (do 
          (<! (timeout processing-delay))
          (swap! results update :processed conj work)
          (recur))))
    
    (Thread/sleep 2000)
    @results))

^{::clerk/visibility {:result :show}}
{:small-buffer (create-backpressure-demo 2 50)
 :large-buffer (create-backpressure-demo 10 50)}

;; ## Circuit Breaker Pattern

^{::clerk/visibility {:result :show}}
(defn circuit-breaker [failure-threshold reset-timeout]
  (let [state (atom {:status :closed :failures 0 :last-failure nil})]
    (fn [operation]
      (go 
        (case (:status @state)
          :closed
          (try 
            (let [result (<! (operation))]
              (if (instance? Throwable result)
                (do 
                  (swap! state update :failures inc)
                  (swap! state assoc :last-failure (System/currentTimeMillis))
                  (when (>= (:failures @state) failure-threshold)
                    (swap! state assoc :status :open))
                  {:error "Operation failed"})
                (do 
                  (swap! state assoc :failures 0)
                  result)))
            (catch Exception e
              (swap! state update :failures inc)
              (swap! state assoc :last-failure (System/currentTimeMillis))
              (when (>= (:failures @state) failure-threshold)
                (swap! state assoc :status :open))
              {:error (.getMessage e)}))
          
          :open
          (if (and (:last-failure @state)
                   (> (System/currentTimeMillis) 
                      (+ (:last-failure @state) reset-timeout)))
            (do 
              (swap! state assoc :status :half-open)
              (<! ((circuit-breaker failure-threshold reset-timeout) operation)))
            {:error "Circuit breaker is open"})
          
          :half-open
          (try 
            (let [result (<! (operation))]
              (if (instance? Throwable result)
                (do 
                  (swap! state assoc :status :open :last-failure (System/currentTimeMillis))
                  {:error "Half-open test failed"})
                (do 
                  (swap! state assoc :status :closed :failures 0)
                  result)))
            (catch Exception e
              (swap! state assoc :status :open :last-failure (System/currentTimeMillis))
              {:error "Half-open test failed"})))))))

^{::clerk/visibility {:result :show}}
(let [failure-count (atom 0)
      breaker (circuit-breaker 3 1000) ; 3 failures, 1 second reset
      results (atom [])]
  
  (defn flaky-operation []
    (go 
      (swap! failure-count inc)
      (if (< @failure-count 5)
        (throw (ex-info "Simulated failure" {}))
        "Success!")))
  
  ;; Test the circuit breaker
  (go 
    (doseq [i (range 8)]
      (let [result (<! (breaker flaky-operation))]
        (swap! results conj {:attempt i :result result}))
      (<! (timeout 200))))
  
  (Thread/sleep 2000)
  @results)

;; ## Retry Pattern with Exponential Backoff

^{::clerk/visibility {:result :show}}
(defn retry-with-backoff [operation max-retries initial-delay]
  (go-loop [attempt 0 delay initial-delay]
    (if (< attempt max-retries)
      (let [result (<! (operation))]
        (if (instance? Throwable result)
          (do 
            (<! (timeout delay))
            (recur (inc attempt) (* delay 2))) ; Exponential backoff
          result))
      {:error "Max retries exceeded"})))

^{::clerk/visibility {:result :show}}
(let [attempt-count (atom 0)
      results (atom [])]
  
  (defn unreliable-operation []
    (go 
      (swap! attempt-count inc)
      (swap! results conj {:attempt @attempt-count :timestamp (System/currentTimeMillis)})
      (if (< @attempt-count 4)
        (ex-info "Still failing" {})
        "Finally succeeded!")))
  
  (<!! (retry-with-backoff unreliable-operation 5 100))
  
  ;; Calculate delays between attempts
  (let [timestamps (map :timestamp @results)
        delays (map - (rest timestamps) timestamps)]
    {:attempts @results
     :delays delays}))

;; ## Timeout Pattern

^{::clerk/visibility {:result :show}}
(defn with-timeout [operation timeout-ms]
  (go 
    (let [timeout-ch (timeout timeout-ms)
          [result channel] (alts! [(operation) timeout-ch])]
      (if (= channel timeout-ch)
        {:error "Operation timed out"}
        result))))

^{::clerk/visibility {:result :show}}
(let [results (atom {})]
  
  (defn slow-operation [delay]
    (go 
      (<! (timeout delay))
      "Operation completed"))
  
  ;; Test with different delays
  (go 
    (swap! results assoc :fast (<! (with-timeout (slow-operation 50) 100))))
  
  (go 
    (swap! results assoc :slow (<! (with-timeout (slow-operation 150) 100))))
  
  (Thread/sleep 200)
  @results)

;; ## Worker Pool Pattern

^{::clerk/visibility {:result :show}}
(defn worker-pool [work-ch result-ch worker-count process-fn]
  "Creates a pool of workers that process items from work-ch"
  (dotimes [i worker-count]
    (go-loop []
      (if-let [work-item (<! work-ch)]
        (let [result (process-fn work-item)]
          (>! result-ch {:worker i :input work-item :result result})
          (recur))))))

^{::clerk/visibility {:result :show}}
(let [work-ch (chan)
      result-ch (chan)
      results (atom [])]
  
  ;; Create worker pool
  (worker-pool work-ch result-ch 3 
               (fn [x] 
                 (Thread/sleep 100) ; Simulate work
                 (* x x)))
  
  ;; Collect results
  (go-loop []
    (if-let [result (<! result-ch)]
      (do 
        (swap! results conj result)
        (recur))))
  
  ;; Submit work
  (go 
    (doseq [i (range 1 11)]
      (>! work-ch i))
    (close! work-ch))
  
  (Thread/sleep 500)
  (sort-by :input @results))

;; ## Pub-Sub Pattern

^{::clerk/visibility {:result :show}}
(defn create-pubsub-demo []
  (let [event-ch (chan)
        pub-ch (async/pub event-ch :topic)
        results (atom {:user-events [] :system-events [] :admin-events []})]
    
    ;; Subscribers for different topics
    (let [user-sub (chan)
          system-sub (chan)
          admin-sub (chan)]
      
      (async/sub pub-ch :user user-sub)
      (async/sub pub-ch :system system-sub)
      (async/sub pub-ch :admin admin-sub)
      
      ;; User event subscriber
      (go-loop []
        (if-let [event (<! user-sub)]
          (do 
            (swap! results update :user-events conj event)
            (recur))))
      
      ;; System event subscriber  
      (go-loop []
        (if-let [event (<! system-sub)]
          (do 
            (swap! results update :system-events conj event)
            (recur))))
      
      ;; Admin event subscriber (gets all events)
      (go-loop []
        (if-let [event (<! admin-sub)]
          (do 
            (swap! results update :admin-events conj event)
            (recur))))
      
      ;; Publish events
      (go 
        (>! event-ch {:topic :user :data "User logged in"})
        (>! event-ch {:topic :system :data "System started"})
        (>! event-ch {:topic :user :data "User updated profile"})
        (>! event-ch {:topic :admin :data "Admin action performed"})
        (>! event-ch {:topic :system :data "System backup completed"})
        (close! event-ch)))
    
    (Thread/sleep 100)
    @results))

^{::clerk/visibility {:result :show}}
(create-pubsub-demo)

;; ## Resource Pool Pattern

^{::clerk/visibility {:result :show}}
(defn resource-pool [resources]
  "Creates a pool of shared resources"
  (let [pool (chan (count resources))]
    ;; Fill the pool
    (go 
      (doseq [resource resources]
        (>! pool resource)))
    pool))

^{::clerk/visibility {:result :show}}
(defn with-resource [pool operation]
  "Borrows a resource from the pool for the duration of the operation"
  (go 
    (if-let [resource (<! pool)]
      (try 
        (let [result (<! (operation resource))]
          (>! pool resource) ; Return resource to pool
          result)
        (catch Exception e
          (>! pool resource) ; Always return resource
          (throw e)))
      {:error "No resources available"})))

^{::clerk/visibility {:result :show}}
(let [db-pool (resource-pool ["conn-1" "conn-2" "conn-3"])
      results (atom [])]
  
  (defn db-operation [connection data]
    (go 
      (<! (timeout 100)) ; Simulate DB work
      {:connection connection :processed data :timestamp (System/currentTimeMillis)}))
  
  ;; Use resources concurrently
  (go 
    (let [futures (for [i (range 5)]
                    (with-resource db-pool #(db-operation % (str "data-" i))))]
      (doseq [future futures]
        (swap! results conj (<! future)))))
  
  (Thread/sleep 300)
  (sort-by :timestamp @results))

;; ## Coordinated Shutdown

^{::clerk/visibility {:result :show}}
(defn coordinated-shutdown [components]
  "Gracefully shuts down multiple components"
  (let [shutdown-ch (chan)
        results (atom {})]
    
    ;; Each component listens for shutdown
    (doseq [[name component-fn] components]
      (go 
        (<! shutdown-ch)
        (let [shutdown-result (<! (component-fn))]
          (swap! results assoc name shutdown-result))))
    
    ;; Trigger shutdown
    (go 
      (<! (timeout 100)) ; Let components start
      (close! shutdown-ch)) ; Broadcast shutdown
    
    {:shutdown-trigger shutdown-ch :results results}))

^{::clerk/visibility {:result :show}}
(let [components {:database (fn [] (go 
                                     (<! (timeout 50))
                                     "Database closed"))
                  :web-server (fn [] (go 
                                       (<! (timeout 30))
                                       "Web server stopped"))
                  :cache (fn [] (go 
                                  (<! (timeout 20))
                                  "Cache cleared"))}
      {:keys [shutdown-trigger results]} (coordinated-shutdown components)]
  
  (Thread/sleep 200)
  @results)

;; ## Summary

;; This notebook demonstrated essential async patterns:

;; - **Rate Limiting**: Controlling operation frequency
;; - **Backpressure**: Managing slow consumers
;; - **Circuit Breaker**: Failing fast when services are down
;; - **Retry with Backoff**: Resilient error recovery
;; - **Timeout**: Preventing hanging operations
;; - **Worker Pool**: Parallel processing with limited resources
;; - **Pub-Sub**: Decoupled event-driven communication
;; - **Resource Pool**: Sharing limited resources safely
;; - **Coordinated Shutdown**: Graceful system termination

;; These patterns form the building blocks for robust, scalable asynchronous systems.
;; Next, we'll explore the flow abstraction for orchestrating complex async workflows.