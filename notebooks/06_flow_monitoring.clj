;; # Flow Monitoring and Debugging
;; This notebook demonstrates techniques for monitoring and debugging async flows.

(ns notebooks.06-flow-monitoring
  (:require [nextjournal.clerk :as clerk]
            [clojure.core.async :as async :refer [go go-loop <! >! <!! >!! chan close! 
                                                  timeout alts! mult tap untap
                                                  pipe pipeline pipeline-blocking
                                                  merge mix admix unmix toggle]]
            [clojure.core.async.impl.protocols :as protocols]))

;; ## Flow Monitoring Infrastructure

;; ### Basic Flow Monitor

^{::clerk/visibility {:result :show}}
(defn create-flow-monitor []
  "Creates a monitoring system for async flows"
  (let [events-ch (chan)
        events (atom [])
        metrics (atom {:total-events 0 
                       :events-by-type {}
                       :start-time (System/currentTimeMillis)})]
    
    ;; Event collector
    (go-loop []
      (if-let [event (<! events-ch)]
        (let [timestamped-event (assoc event :timestamp (System/currentTimeMillis))]
          (swap! events conj timestamped-event)
          (swap! metrics update :total-events inc)
          (swap! metrics update-in [:events-by-type (:type event)] (fnil inc 0))
          (recur))))
    
    {:events-channel events-ch
     :events events  
     :metrics metrics
     :log-event (fn [event-type data]
                  (go (>! events-ch {:type event-type :data data})))}))

^{::clerk/visibility {:result :show}}
(let [monitor (create-flow-monitor)
      log-fn (:log-event monitor)]
  
  ;; Simulate some events
  (go 
    (log-fn :flow-start {:flow-id "demo-flow"})
    (<! (timeout 50))
    (log-fn :processing {:node "step-1" :input 42})
    (<! (timeout 30))
    (log-fn :processing {:node "step-2" :input 84})
    (<! (timeout 20))
    (log-fn :flow-complete {:flow-id "demo-flow" :result 168}))
  
  (Thread/sleep 200)
  {:total-events (:total-events @(:metrics monitor))
   :events-by-type (:events-by-type @(:metrics monitor))
   :recent-events (take 5 @(:events monitor))})

;; ### Channel Monitoring

^{::clerk/visibility {:result :show}}
(defn monitored-channel [base-ch monitor label]
  "Wraps a channel with monitoring capabilities"
  (let [monitored-ch (chan)]
    
    ;; Monitor puts
    (go-loop [put-count 0]
      (if-let [item (<! base-ch)]
        (do 
          ((:log-event monitor) :channel-put {:label label :item item :count (inc put-count)})
          (>! monitored-ch item)
          (recur (inc put-count)))
        (do 
          ((:log-event monitor) :channel-closed {:label label :total-puts put-count})
          (close! monitored-ch))))
    
    monitored-ch))

^{::clerk/visibility {:result :show}}
(let [monitor (create-flow-monitor)
      base-ch (chan)
      mon-ch (monitored-channel base-ch monitor "demo-channel")
      results (atom [])]
  
  ;; Consumer
  (go-loop []
    (if-let [item (<! mon-ch)]
      (do 
        (swap! results conj item)
        (recur))))
  
  ;; Producer
  (go 
    (doseq [i (range 5)]
      (>! base-ch i)
      (<! (timeout 20)))
    (close! base-ch))
  
  (Thread/sleep 200)
  {:results @results
   :monitoring-events (map #(select-keys % [:type :data]) @(:events monitor))})

;; ### Performance Monitoring

^{::clerk/visibility {:result :show}}
(defn performance-monitor [operation-fn]
  "Monitors performance of async operations"
  (let [stats (atom {:call-count 0 
                     :total-time 0
                     :min-time Long/MAX_VALUE
                     :max-time 0
                     :errors 0})]
    
    (fn [& args]
      (go 
        (let [start-time (System/currentTimeMillis)]
          (try 
            (let [result (<! (apply operation-fn args))
                  end-time (System/currentTimeMillis)
                  duration (- end-time start-time)]
              
              (swap! stats update :call-count inc)
              (swap! stats update :total-time + duration)
              (swap! stats update :min-time min duration)
              (swap! stats update :max-time max duration)
              
              {:result result :duration duration :stats @stats})
            
            (catch Exception e
              (swap! stats update :errors inc)
              {:error (.getMessage e) :stats @stats})))))))

^{::clerk/visibility {:result :show}}
(let [slow-operation (fn [x] 
                       (go 
                         (<! (timeout (+ 50 (rand-int 100))))
                         (* x x)))
      monitored-op (performance-monitor slow-operation)
      results (atom [])]
  
  (go 
    (doseq [i (range 5)]
      (let [result (<! (monitored-op i))]
        (swap! results conj result))))
  
  (Thread/sleep 1000)
  (let [final-results @results
        final-stats (:stats (last final-results))]
    {:individual-results (map #(select-keys % [:result :duration]) final-results)
     :aggregate-stats (assoc final-stats :avg-time (/ (:total-time final-stats) (:call-count final-stats)))}))

;; ### Flow Execution Tracer

^{::clerk/visibility {:result :show}}
(defn flow-tracer [flow-definition]
  "Traces execution path through a flow"
  (let [trace (atom [])
        node-stats (atom {})]
    
    (letfn [(trace-node [node-id input]
              (go 
                (let [start-time (System/currentTimeMillis)
                      node-def (get flow-definition node-id)
                      result (<! ((:fn node-def) input))
                      end-time (System/currentTimeMillis)
                      duration (- end-time start-time)]
                  
                  (swap! trace conj {:node node-id 
                                     :input input 
                                     :output result
                                     :duration duration
                                     :timestamp start-time})
                  
                  (swap! node-stats update node-id 
                         (fn [stats] 
                           (-> (or stats {:calls 0 :total-time 0})
                               (update :calls inc)
                               (update :total-time + duration))))
                  
                  result)))]
      
      {:trace-fn trace-node
       :get-trace #(deref trace)
       :get-stats #(deref node-stats)})))

^{::clerk/visibility {:result :show}}
(let [flow-def {:step1 {:fn (fn [x] (go (<! (timeout 30)) (* x 2)))}
                :step2 {:fn (fn [x] (go (<! (timeout 20)) (+ x 10)))}
                :step3 {:fn (fn [x] (go (<! (timeout 40)) (/ x 2)))}}
      tracer (flow-tracer flow-def)
      trace-fn (:trace-fn tracer)]
  
  (go 
    (let [input 5
          result1 (<! (trace-fn :step1 input))
          result2 (<! (trace-fn :step2 result1))
          result3 (<! (trace-fn :step3 result2))]
      result3))
  
  (Thread/sleep 200)
  {:execution-trace ((:get-trace tracer))
   :node-statistics ((:get-stats tracer))})

;; ### Error Tracking and Analysis

^{::clerk/visibility {:result :show}}
(defn error-tracker []
  "Tracks and analyzes errors in async flows"
  (let [errors (atom [])
        error-stats (atom {:total-errors 0
                          :errors-by-type {}
                          :errors-by-node {}})]
    
    {:track-error (fn [error-type node-id error-data]
                    (let [error-record {:type error-type
                                       :node node-id  
                                       :data error-data
                                       :timestamp (System/currentTimeMillis)}]
                      (swap! errors conj error-record)
                      (swap! error-stats update :total-errors inc)
                      (swap! error-stats update-in [:errors-by-type error-type] (fnil inc 0))
                      (swap! error-stats update-in [:errors-by-node node-id] (fnil inc 0))))
     
     :get-errors #(deref errors)
     :get-stats #(deref error-stats)
     :get-recent-errors (fn [n] (take n (reverse @errors)))}))

^{::clerk/visibility {:result :show}}
(let [error-tracker (error-tracker)
      track-fn (:track-error error-tracker)]
  
  ;; Simulate some errors
  (track-fn :validation-error "input-validator" {:message "Invalid input format"})
  (track-fn :timeout-error "external-service" {:timeout 5000})
  (track-fn :validation-error "input-validator" {:message "Missing required field"})
  (track-fn :processing-error "data-processor" {:exception "NullPointerException"})
  (track-fn :timeout-error "database" {:timeout 3000})
  
  {:total-errors (get-in ((:get-stats error-tracker)) [:total-errors])
   :error-breakdown (:errors-by-type ((:get-stats error-tracker)))
   :node-breakdown (:errors-by-node ((:get-stats error-tracker)))
   :recent-errors ((:get-recent-errors error-tracker) 3)})

;; ### Real-time Flow Metrics Dashboard

^{::clerk/visibility {:result :show}}
(defn flow-dashboard []
  "Creates a real-time dashboard for flow metrics"
  (let [metrics (atom {:active-flows {}
                       :completed-flows 0
                       :failed-flows 0
                       :total-processing-time 0
                       :average-flow-time 0})
        update-ch (chan)]
    
    ;; Metrics updater
    (go-loop []
      (if-let [update (<! update-ch)]
        (case (:type update)
          :flow-started 
          (swap! metrics assoc-in [:active-flows (:flow-id update)] 
                 {:start-time (System/currentTimeMillis) :status :running})
          
          :flow-completed
          (let [flow-info (get-in @metrics [:active-flows (:flow-id update)])
                duration (- (System/currentTimeMillis) (:start-time flow-info))]
            (swap! metrics update :completed-flows inc)
            (swap! metrics update :total-processing-time + duration)
            (swap! metrics update-in [:active-flows] dissoc (:flow-id update))
            (let [completed (:completed-flows @metrics)]
              (swap! metrics assoc :average-flow-time 
                     (/ (:total-processing-time @metrics) completed))))
          
          :flow-failed
          (do 
            (swap! metrics update :failed-flows inc)
            (swap! metrics update-in [:active-flows] dissoc (:flow-id update))))
        (recur)))
    
    {:update-channel update-ch
     :get-metrics #(deref metrics)
     :start-flow (fn [flow-id] (go (>! update-ch {:type :flow-started :flow-id flow-id})))
     :complete-flow (fn [flow-id] (go (>! update-ch {:type :flow-completed :flow-id flow-id})))
     :fail-flow (fn [flow-id] (go (>! update-ch {:type :flow-failed :flow-id flow-id})))}))

^{::clerk/visibility {:result :show}}
(let [dashboard (flow-dashboard)
      start-fn (:start-flow dashboard)
      complete-fn (:complete-flow dashboard)
      fail-fn (:fail-flow dashboard)]
  
  ;; Simulate flow lifecycle events
  (go 
    (start-fn "flow-1")
    (<! (timeout 100))
    (complete-fn "flow-1")
    
    (start-fn "flow-2") 
    (start-fn "flow-3")
    (<! (timeout 50))
    (fail-fn "flow-2")
    (<! (timeout 80))
    (complete-fn "flow-3"))
  
  (Thread/sleep 300)
  ((:get-metrics dashboard)))

;; ### Bottleneck Detection

^{::clerk/visibility {:result :show}}
(defn bottleneck-detector [channels sample-interval-ms threshold-ratio]
  "Detects bottlenecks by monitoring channel buffer usage"
  (let [measurements (atom {})
        bottlenecks (atom [])]
    
    (go-loop []
      (<! (timeout sample-interval-ms))
      (doseq [[name ch-info] channels]
        (let [buffer-size (:buffer-size ch-info 1)
              ;; Simulated buffer usage (in real implementation, would check actual buffer)
              current-usage (rand-int (inc buffer-size))
              usage-ratio (/ current-usage buffer-size)]
          
          (swap! measurements assoc name {:usage current-usage 
                                         :capacity buffer-size
                                         :ratio usage-ratio
                                         :timestamp (System/currentTimeMillis)})
          
          (when (> usage-ratio threshold-ratio)
            (swap! bottlenecks conj {:channel name
                                   :usage-ratio usage-ratio  
                                   :timestamp (System/currentTimeMillis)}))))
      (recur))
    
    {:get-measurements #(deref measurements)
     :get-bottlenecks #(deref bottlenecks)
     :clear-bottlenecks #(reset! bottlenecks [])}))

^{::clerk/visibility {:result :show}}
(let [channels {:input-queue {:buffer-size 10}
                :processing-queue {:buffer-size 5}
                :output-queue {:buffer-size 20}}
      detector (bottleneck-detector channels 100 0.8)]
  
  (Thread/sleep 500) ; Let detector run for a bit
  
  {:current-measurements ((:get-measurements detector))
   :detected-bottlenecks (take 3 ((:get-bottlenecks detector)))})

;; ### Flow Health Checker

^{::clerk/visibility {:result :show}}
(defn flow-health-checker [health-checks check-interval-ms]
  "Monitors overall flow health"
  (let [health-status (atom {})
        alerts (atom [])]
    
    (go-loop []
      (doseq [[check-name check-fn] health-checks]
        (try 
          (let [result (<! (check-fn))]
            (swap! health-status assoc check-name 
                   {:status :healthy 
                    :last-check (System/currentTimeMillis)
                    :details result}))
          (catch Exception e
            (swap! health-status assoc check-name 
                   {:status :unhealthy
                    :last-check (System/currentTimeMillis)
                    :error (.getMessage e)})
            (swap! alerts conj {:check check-name 
                               :error (.getMessage e)
                               :timestamp (System/currentTimeMillis)}))))
      
      (<! (timeout check-interval-ms))
      (recur))
    
    {:get-health #(deref health-status)
     :get-alerts #(deref alerts)
     :is-healthy? (fn [] (every? #(= :healthy (:status %)) (vals @health-status)))}))

^{::clerk/visibility {:result :show}}
(let [health-checks {:memory-usage (fn [] (go {:free-memory (Runtime/getRuntime)
                                              :used-memory "simulated"}))
                     :channel-connectivity (fn [] (go {:channels-connected 5}))
                     :error-rate (fn [] (go {:errors-per-minute 2}))}
      checker (flow-health-checker health-checks 200)]
  
  (Thread/sleep 500)
  
  {:overall-health ((:is-healthy? checker))
   :health-details ((:get-health checker))
   :alerts (take 2 ((:get-alerts checker)))})

;; ### Comprehensive Flow Debugger

^{::clerk/visibility {:result :show}}
(defn flow-debugger [flow-config]
  "Comprehensive debugging framework for async flows"
  (let [debug-info (atom {:breakpoints #{}
                         :step-mode false
                         :execution-log []
                         :variable-watch {}
                         :call-stack []})]
    
    {:set-breakpoint (fn [node-id] 
                       (swap! debug-info update :breakpoints conj node-id))
     
     :remove-breakpoint (fn [node-id]
                          (swap! debug-info update :breakpoints disj node-id))
     
     :enable-step-mode (fn [] 
                         (swap! debug-info assoc :step-mode true))
     
     :watch-variable (fn [var-name value]
                       (swap! debug-info assoc-in [:variable-watch var-name] value))
     
     :execute-with-debug (fn [node-id input continue-ch]
                           (go 
                             (let [is-breakpoint (contains? (:breakpoints @debug-info) node-id)]
                               (swap! debug-info update :execution-log conj 
                                      {:node node-id :input input :timestamp (System/currentTimeMillis)})
                               
                               (when (or is-breakpoint (:step-mode @debug-info))
                                 ;; Wait for continue signal
                                 (<! continue-ch))
                               
                               ;; Execute actual node logic here
                               (let [result (* input 2)] ; Simulated processing
                                 (swap! debug-info update :execution-log conj 
                                        {:node node-id :output result :timestamp (System/currentTimeMillis)})
                                 result))))
     
     :get-debug-info #(deref debug-info)
     :clear-log #(swap! debug-info assoc :execution-log [])}))

^{::clerk/visibility {:result :show}}
(let [debugger (flow-debugger {})
      continue-ch (chan)]
  
  ;; Set up debugging
  ((:set-breakpoint debugger) :step2)
  ((:enable-step-mode debugger))
  
  ;; Simulate execution
  (go 
    (let [result1 (<! ((:execute-with-debug debugger) :step1 10 continue-ch))
          _ (>! continue-ch :continue) ; Continue past step1
          result2 (<! ((:execute-with-debug debugger) :step2 result1 continue-ch))
          _ (>! continue-ch :continue) ; Continue past step2 breakpoint  
          result3 (<! ((:execute-with-debug debugger) :step3 result2 continue-ch))]
      (>! continue-ch :continue))) ; Final continue
  
  (Thread/sleep 100)
  (select-keys ((:get-debug-info debugger)) [:breakpoints :step-mode :execution-log]))

;; ## Summary

;; This notebook demonstrated comprehensive monitoring and debugging techniques:

;; - **Flow Monitor**: Central event logging and metrics collection
;; - **Channel Monitoring**: Tracking channel operations and lifecycle
;; - **Performance Monitoring**: Measuring operation timing and statistics  
;; - **Execution Tracing**: Following data flow through processing steps
;; - **Error Tracking**: Collecting and analyzing error patterns
;; - **Real-time Dashboard**: Live metrics and flow status
;; - **Bottleneck Detection**: Identifying performance constraints
;; - **Health Checking**: Monitoring overall system health
;; - **Interactive Debugging**: Breakpoints, stepping, and inspection

;; These tools enable comprehensive observability and debugging of complex
;; async flows, making it easier to develop, deploy, and maintain robust
;; asynchronous systems in production.