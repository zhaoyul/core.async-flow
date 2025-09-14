;; # Advanced Flow Orchestration
;; This notebook explores advanced patterns for orchestrating complex data flows.

(ns notebooks.05-flow-orchestration
  (:require [nextjournal.clerk :as clerk]
            [clojure.core.async :as async :refer [go go-loop <! >! <!! >!! chan close! 
                                                  timeout alts! mult tap untap
                                                  pipe pipeline pipeline-blocking
                                                  merge mix admix unmix toggle]]
            [clojure.core.async.impl.protocols :as protocols]))

;; ## Advanced Flow Patterns

;; ### Dynamic Flow Configuration

^{::clerk/visibility {:result :show}}
(defn create-dynamic-flow [flow-config]
  "Creates a flow based on runtime configuration"
  (let [nodes (atom {})
        channels (atom {})]
    
    (doseq [node-config flow-config]
      (let [node-id (:id node-config)
            input-ch (chan)
            output-ch (chan)
            process-fn (:fn node-config)]
        
        (swap! nodes assoc node-id node-config)
        (swap! channels assoc node-id {:input input-ch :output output-ch})
        
        ;; Start processing for this node
        (go-loop []
          (if-let [input (<! input-ch)]
            (let [result (process-fn input)]
              (>! output-ch result)
              (recur))
            (close! output-ch)))))
    
    {:nodes @nodes :channels @channels}))

^{::clerk/visibility {:result :show}}
(let [flow-config [{:id :double :fn #(* 2 %)}
                   {:id :square :fn #(* % %)}
                   {:id :add-100 :fn #(+ % 100)}]
      flow (create-dynamic-flow flow-config)]
  
  {:node-count (count (:nodes flow))
   :channel-count (count (:channels flow))
   :node-ids (keys (:nodes flow))})

;; ### Conditional Flow Routing

^{::clerk/visibility {:result :show}}
(defn conditional-router [input-ch route-fn route-channels]
  "Routes messages to different channels based on condition"
  (go-loop []
    (if-let [input (<! input-ch)]
      (let [route (route-fn input)
            target-ch (get route-channels route)]
        (when target-ch
          (>! target-ch input))
        (recur))
      (doseq [ch (vals route-channels)]
        (close! ch)))))

^{::clerk/visibility {:result :show}}
(let [input-ch (chan)
      even-ch (chan)
      odd-ch (chan) 
      large-ch (chan)
      results (atom {:even [] :odd [] :large []})]
  
  ;; Set up routing
  (conditional-router input-ch 
                     (fn [x] 
                       (cond 
                         (> x 100) :large
                         (even? x) :even
                         :else :odd))
                     {:even even-ch :odd odd-ch :large large-ch})
  
  ;; Collect results from each route
  (go-loop []
    (if-let [value (<! even-ch)]
      (do 
        (swap! results update :even conj value)
        (recur))))
  
  (go-loop []
    (if-let [value (<! odd-ch)]
      (do 
        (swap! results update :odd conj value)
        (recur))))
  
  (go-loop []
    (if-let [value (<! large-ch)]
      (do 
        (swap! results update :large conj value)
        (recur))))
  
  ;; Send test data
  (go 
    (doseq [i [1 2 3 4 150 6 7 8 200 10]]
      (>! input-ch i))
    (close! input-ch))
  
  (Thread/sleep 100)
  @results)

;; ### Flow with Compensation

^{::clerk/visibility {:result :show}}
(defn compensating-transaction [operations compensations input]
  "Executes operations with compensation on failure"
  (go 
    (loop [executed []
           remaining-ops operations
           remaining-comps compensations
           current-input input]
      (if-let [op (first remaining-ops)]
        (try 
          (let [result (<! (op current-input))]
            (if (instance? Throwable result)
              ;; Operation failed, run compensations
              (do 
                (doseq [comp (reverse executed)]
                  (<! (comp)))
                {:status :failed 
                 :error (.getMessage result)
                 :compensated (count executed)})
              ;; Operation succeeded
              (recur (conj executed (first remaining-comps))
                     (rest remaining-ops)
                     (rest remaining-comps)
                     result)))
          (catch Exception e
            (doseq [comp (reverse executed)]
              (<! (comp)))
            {:status :error 
             :error (.getMessage e)
             :compensated (count executed)}))
        {:status :success :result current-input}))))

^{::clerk/visibility {:result :show}}
(let [operations [(fn [x] (go 
                            (<! (timeout 50))
                            (if (> x 0) 
                              (* x 2)
                              (throw (ex-info "Invalid input" {})))))
                  (fn [x] (go 
                            (<! (timeout 30))
                            (+ x 10)))
                  (fn [x] (go 
                            (<! (timeout 20))
                            (/ x 2)))]
      
      compensations [(fn [] (go (println "Compensating step 1")))
                     (fn [] (go (println "Compensating step 2")))
                     (fn [] (go (println "Compensating step 3")))]]
  
  {;; Success case
   :success-case (<!! (compensating-transaction operations compensations 5))
   ;; Failure case
   :failure-case (<!! (compensating-transaction operations compensations -1))})

;; ### Flow Orchestration with State Machine

^{::clerk/visibility {:result :show}}
(defrecord FlowState [current-state transitions data])

^{::clerk/visibility {:result :show}}
(defn state-machine-flow [initial-state transitions input-ch]
  "Flow controlled by state machine"
  (let [output-ch (chan)
        state (atom (->FlowState initial-state transitions {}))]
    
    (go-loop []
      (if-let [input (<! input-ch)]
        (let [current-state (:current-state @state)
              available-transitions (get (:transitions @state) current-state)
              transition (first (filter #((:condition %) input) available-transitions))]
          
          (if transition
            (let [new-state (:to transition)
                  result ((:action transition) input (:data @state))]
              (swap! state assoc :current-state new-state)
              (swap! state update :data merge (:new-data result {}))
              (>! output-ch {:state new-state :result (:result result) :input input})
              (recur))
            (do 
              (>! output-ch {:error "No valid transition" :state current-state :input input})
              (recur))))
        (close! output-ch)))
    
    {:output-channel output-ch :state state}))

^{::clerk/visibility {:result :show}}
(let [input-ch (chan)
      transitions {:idle [{:condition #(> % 0) 
                          :action (fn [input data] {:result (* input 2)})
                          :to :processing}]
                  :processing [{:condition #(even? %) 
                               :action (fn [input data] {:result (+ input 100)})
                               :to :complete}
                              {:condition #(odd? %) 
                               :action (fn [input data] {:result (* input 3)})
                               :to :idle}]
                  :complete [{:condition (constantly true)
                             :action (fn [input data] {:result "done"})
                             :to :idle}]}
      
      flow (state-machine-flow :idle transitions input-ch)
      results (atom [])]
  
  ;; Collect outputs
  (go-loop []
    (if-let [output (<! (:output-channel flow))]
      (do 
        (swap! results conj output)
        (recur))))
  
  ;; Send inputs
  (go 
    (doseq [i [1 2 3 4 5]]
      (>! input-ch i)
      (<! (timeout 50)))
    (close! input-ch))
  
  (Thread/sleep 300)
  @results)

;; ### Load Balancing Flow

^{::clerk/visibility {:result :show}}
(defn load-balanced-flow [worker-fns input-ch]
  "Distributes work across multiple workers"
  (let [output-ch (chan)
        worker-channels (mapv (fn [worker-fn]
                               (let [worker-ch (chan)]
                                 (go-loop []
                                   (if-let [work (<! worker-ch)]
                                     (let [result (<! (worker-fn work))]
                                       (>! output-ch {:worker worker-fn :input work :result result})
                                       (recur))
                                     (println "Worker shutting down")))
                                 worker-ch))
                             worker-fns)
        worker-index (atom 0)]
    
    ;; Distribute work round-robin
    (go-loop []
      (if-let [work (<! input-ch)]
        (let [worker-ch (nth worker-channels (mod @worker-index (count worker-channels)))]
          (swap! worker-index inc)
          (>! worker-ch work)
          (recur))
        (doseq [ch worker-channels]
          (close! ch))))
    
    output-ch))

^{::clerk/visibility {:result :show}}
(let [input-ch (chan)
      worker-fns [(fn [x] (go (<! (timeout 100)) (* x x)))    ; Slow worker
                  (fn [x] (go (<! (timeout 50)) (+ x 10)))    ; Fast worker  
                  (fn [x] (go (<! (timeout 75)) (- x 1)))]    ; Medium worker
      
      output-ch (load-balanced-flow worker-fns input-ch)
      results (atom [])]
  
  ;; Collect results
  (go-loop []
    (if-let [result (<! output-ch)]
      (do 
        (swap! results conj result)
        (recur))))
  
  ;; Send work
  (go 
    (doseq [i (range 1 10)]
      (>! input-ch i))
    (close! input-ch))
  
  (Thread/sleep 500)
  (group-by #(hash (:worker %)) @results))

;; ### Flow with Priority Queuing

^{::clerk/visibility {:result :show}}
(defn priority-flow [input-ch priority-fn]
  "Processes items based on priority"
  (let [output-ch (chan)
        priority-queues (atom {})]
    
    ;; Input processor - sorts into priority queues
    (go-loop []
      (if-let [item (<! input-ch)]
        (let [priority (priority-fn item)]
          (swap! priority-queues update priority (fnil conj []) item)
          (recur))))
    
    ;; Output processor - processes highest priority first
    (go-loop []
      (if-let [highest-priority (and (seq @priority-queues)
                                    (apply max (keys @priority-queues)))]
        (let [queue (get @priority-queues highest-priority)]
          (if (seq queue)
            (let [item (first queue)]
              (swap! priority-queues update highest-priority rest)
              (when (empty? (get @priority-queues highest-priority))
                (swap! priority-queues dissoc highest-priority))
              (>! output-ch {:item item :priority highest-priority})
              (<! (timeout 50)) ; Simulate processing time
              (recur))
            (do 
              (<! (timeout 10))
              (recur))))
        (do 
          (<! (timeout 100))
          (when (seq @priority-queues)
            (recur)))))
    
    output-ch))

^{::clerk/visibility {:result :show}}
(let [input-ch (chan)
      priority-fn (fn [item] 
                    (cond 
                      (> item 50) 3  ; High priority
                      (> item 20) 2  ; Medium priority
                      :else 1))      ; Low priority
      output-ch (priority-flow input-ch priority-fn)
      results (atom [])]
  
  ;; Collect results
  (go-loop []
    (if-let [result (<! output-ch)]
      (do 
        (swap! results conj result)
        (recur))))
  
  ;; Send mixed priority items
  (go 
    (doseq [i [10 60 5 80 15 70 25 90 3]]
      (>! input-ch i))
    (close! input-ch))
  
  (Thread/sleep 800)
  @results)

;; ### Adaptive Flow Control

^{::clerk/visibility {:result :show}}
(defn adaptive-flow [input-ch process-fn target-throughput]
  "Adapts processing rate based on throughput targets"
  (let [output-ch (chan)
        metrics (atom {:processed 0 :last-check (System/currentTimeMillis) :delay 10})]
    
    (go-loop []
      (if-let [item (<! input-ch)]
        (let [start-time (System/currentTimeMillis)
              result (<! (process-fn item))
              current-time (System/currentTimeMillis)]
          
          (>! output-ch {:item item :result result :processing-time (- current-time start-time)})
          (swap! metrics update :processed inc)
          
          ;; Adjust delay based on throughput
          (let [time-since-check (- current-time (:last-check @metrics))]
            (when (> time-since-check 1000) ; Check every second
              (let [current-throughput (/ (:processed @metrics) (/ time-since-check 1000.0))]
                (cond 
                  (< current-throughput target-throughput)
                  (swap! metrics update :delay #(max 1 (- % 5))) ; Speed up
                  
                  (> current-throughput target-throughput)
                  (swap! metrics update :delay #(+ % 5))) ; Slow down
                
                (swap! metrics assoc :processed 0 :last-check current-time))))
          
          (<! (timeout (:delay @metrics)))
          (recur))
        (close! output-ch)))
    
    {:output-channel output-ch :metrics metrics}))

^{::clerk/visibility {:result :show}}
(let [input-ch (chan)
      process-fn (fn [x] (go 
                           (<! (timeout (+ 10 (rand-int 20)))) ; Variable processing time
                           (* x 2)))
      flow (adaptive-flow input-ch process-fn 5) ; Target: 5 items per second
      results (atom [])]
  
  ;; Collect results
  (go-loop []
    (if-let [result (<! (:output-channel flow))]
      (do 
        (swap! results conj result)
        (recur))))
  
  ;; Send data
  (go 
    (doseq [i (range 1 21)]
      (>! input-ch i))
    (close! input-ch))
  
  (Thread/sleep 5000)
  {:results (count @results)
   :final-metrics @(:metrics flow)
   :avg-processing-time (if (seq @results)
                         (/ (reduce + (map :processing-time @results))
                            (count @results))
                         0)})

;; ## Summary

;; This notebook explored advanced flow orchestration patterns:

;; - **Dynamic Configuration**: Runtime flow creation and modification
;; - **Conditional Routing**: Message routing based on content
;; - **Compensation**: Rollback mechanisms for failed operations
;; - **State Machines**: Flow control using state transitions
;; - **Load Balancing**: Work distribution across multiple workers
;; - **Priority Queuing**: Processing based on item priority
;; - **Adaptive Control**: Self-adjusting processing rates

;; These advanced patterns enable building sophisticated, resilient,
;; and adaptive data processing systems using async primitives.
;; Next, we'll explore monitoring and debugging these complex flows.