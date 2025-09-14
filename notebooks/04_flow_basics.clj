;; # Flow Basics - Data Flow Orchestration
;; This notebook introduces concepts for orchestrating complex data flows using async patterns.

(ns notebooks.04-flow-basics
  (:require [nextjournal.clerk :as clerk]
            [clojure.core.async :as async :refer [go go-loop <! >! <!! >!! chan close! 
                                                  timeout alts! mult tap untap
                                                  pipe pipeline pipeline-blocking
                                                  merge mix admix unmix toggle]]
            [clojure.core.async.impl.protocols :as protocols]))

;; ## Introduction to Flow Concepts

;; Flow orchestration allows us to define complex data processing pipelines
;; with dependencies, error handling, and monitoring. While core.async.flow
;; may not be a standard library yet, we can implement similar concepts.

;; ### Basic Flow Definition

^{::clerk/visibility {:result :show}}
(defrecord FlowNode [id fn deps input-channels output-channel])

^{::clerk/visibility {:result :show}}
(defn create-flow-node [id process-fn deps]
  "Creates a flow node with specified dependencies"
  (->FlowNode id process-fn deps (atom {}) (chan)))

^{::clerk/visibility {:result :show}}
(defn simple-flow-example []
  (let [; Define processing steps
        step1 (create-flow-node :input #(* % 2) [])
        step2 (create-flow-node :square #(* % %) [:input])
        step3 (create-flow-node :add10 #(+ % 10) [:square])
        step4 (create-flow-node :output identity [:add10])
        
        ; Flow graph
        flow-graph [step1 step2 step3 step4]
        results (atom {})]
    
    {:flow-graph (map #(select-keys % [:id :deps]) flow-graph)
     :description "Input -> *2 -> square -> +10 -> output"}))

^{::clerk/visibility {:result :show}}
(simple-flow-example)

;; ### Flow Execution Engine

^{::clerk/visibility {:result :show}}
(defn execute-flow-node [node input-data dependencies-met?]
  "Executes a flow node if its dependencies are met"
  (go 
    (if dependencies-met?
      (try 
        (let [result ((:fn node) input-data)]
          (>! (:output-channel node) {:status :success :result result :node (:id node)})
          result)
        (catch Exception e
          (>! (:output-channel node) {:status :error :error (.getMessage e) :node (:id node)})
          nil))
      (>! (:output-channel node) {:status :waiting :node (:id node)}))))

^{::clerk/visibility {:result :show}}
(defn simple-linear-flow [input-value]
  "Demonstrates a simple linear flow execution"
  (let [results (atom [])]
    (go 
      ; Step 1: Double the input
      (let [step1-result (* input-value 2)]
        (swap! results conj {:step :double :input input-value :output step1-result})
        
        ; Step 2: Square the result
        (let [step2-result (* step1-result step1-result)]
          (swap! results conj {:step :square :input step1-result :output step2-result})
          
          ; Step 3: Add 10
          (let [step3-result (+ step2-result 10)]
            (swap! results conj {:step :add10 :input step2-result :output step3-result})
            step3-result))))
    
    (Thread/sleep 100)
    {:final-result (<!! (go (last (map :output @results))))
     :execution-trace @results}))

^{::clerk/visibility {:result :show}}
(simple-linear-flow 3)

;; ### Parallel Flow Execution

^{::clerk/visibility {:result :show}}
(defn parallel-flow-demo [input-value]
  "Demonstrates parallel branches in a flow"
  (let [results (atom {})]
    (go 
      ; Branch 1: mathematical operations
      (let [math-result (go 
                          (let [doubled (* input-value 2)]
                            (<! (timeout 50)) ; Simulate processing time
                            (let [squared (* doubled doubled)]
                              (<! (timeout 30))
                              squared)))]
        (swap! results assoc :math-branch (<! math-result)))
      
      ; Branch 2: string operations  
      (let [string-result (go 
                            (let [str-val (str input-value)]
                              (<! (timeout 40))
                              (let [reversed (apply str (reverse str-val))]
                                (<! (timeout 20))
                                reversed)))]
        (swap! results assoc :string-branch (<! string-result)))
      
      ; Branch 3: collection operations
      (let [coll-result (go 
                          (let [coll (repeat input-value input-value)]
                            (<! (timeout 60))
                            (let [summed (reduce + coll)]
                              (<! (timeout 10))
                              summed)))]
        (swap! results assoc :collection-branch (<! coll-result))))
    
    (Thread/sleep 200)
    @results))

^{::clerk/visibility {:result :show}}
(parallel-flow-demo 4)

;; ### Flow with Dependencies

^{::clerk/visibility {:result :show}}
(defn dependency-flow-demo []
  "Demonstrates a flow with complex dependencies"
  (let [results (atom {})
        channels {:a (chan) :b (chan) :c (chan) :d (chan) :e (chan)}]
    
    ; Node A: Independent  
    (go 
      (let [result 10]
        (swap! results assoc :a result)
        (>! (:a channels) result)))
    
    ; Node B: Independent
    (go 
      (let [result 20]  
        (swap! results assoc :b result)
        (>! (:b channels) result)))
    
    ; Node C: Depends on A
    (go 
      (let [a-val (<! (:a channels))
            result (* a-val 2)]
        (swap! results assoc :c result)
        (>! (:c channels) result)))
    
    ; Node D: Depends on B
    (go 
      (let [b-val (<! (:b channels))
            result (+ b-val 5)]
        (swap! results assoc :d result)
        (>! (:d channels) result)))
    
    ; Node E: Depends on both C and D
    (go 
      (let [c-val (<! (:c channels))
            d-val (<! (:d channels))
            result (+ c-val d-val)]
        (swap! results assoc :e result)
        (>! (:e channels) result)))
    
    (Thread/sleep 100)
    {:results @results
     :dependency-graph {:a [] :b [] :c [:a] :d [:b] :e [:c :d]}}))

^{::clerk/visibility {:result :show}}
(dependency-flow-demo)

;; ### Flow with Error Handling

^{::clerk/visibility {:result :show}}
(defn error-handling-flow [input-values]
  "Demonstrates error handling in flows"
  (let [results (atom [])
        error-ch (chan)
        success-ch (chan)]
    
    ; Process each input
    (go 
      (doseq [val input-values]
        (try 
          (let [result (cond 
                         (zero? val) (throw (ex-info "Cannot process zero" {:value val}))
                         (neg? val) (throw (ex-info "Cannot process negative" {:value val}))
                         :else (* val val))]
            (>! success-ch {:input val :result result}))
          (catch Exception e
            (>! error-ch {:input val :error (.getMessage e) :data (ex-data e)})))))
    
    ; Collect successes
    (go-loop []
      (if-let [success (<! success-ch)]
        (do 
          (swap! results conj {:type :success :data success})
          (recur))))
    
    ; Collect errors
    (go-loop []
      (if-let [error (<! error-ch)]
        (do 
          (swap! results conj {:type :error :data error})
          (recur))))
    
    (Thread/sleep 100)
    (group-by :type @results)))

^{::clerk/visibility {:result :show}}
(error-handling-flow [1 2 0 3 -1 4 5])

;; ### Flow State Management

^{::clerk/visibility {:result :show}}
(defn stateful-flow-demo []
  "Demonstrates maintaining state across flow execution"
  (let [state (atom {:processed 0 :total 0 :errors 0})
        results (atom [])
        input-ch (chan)
        output-ch (chan)]
    
    ; State updater
    (go-loop []
      (if-let [input (<! input-ch)]
        (do 
          (swap! state update :total inc)
          (try 
            (let [result (if (even? input)
                           (* input 2)
                           (throw (ex-info "Odd numbers not allowed" {:input input})))]
              (swap! state update :processed inc)
              (>! output-ch {:input input :result result :state @state}))
            (catch Exception e
              (swap! state update :errors inc)
              (>! output-ch {:input input :error (.getMessage e) :state @state})))
          (recur))))
    
    ; Result collector
    (go-loop []
      (if-let [output (<! output-ch)]
        (do 
          (swap! results conj output)
          (recur))))
    
    ; Feed data
    (go 
      (doseq [i (range 1 11)]
        (>! input-ch i))
      (close! input-ch))
    
    (Thread/sleep 100)
    {:final-state @state
     :execution-log @results}))

^{::clerk/visibility {:result :show}}
(stateful-flow-demo)

;; ### Conditional Flow Execution

^{::clerk/visibility {:result :show}}
(defn conditional-flow-demo [condition-fn input-data]
  "Demonstrates conditional branching in flows"
  (let [results (atom {})]
    (go 
      (if (condition-fn input-data)
        ; Path A: Complex processing
        (let [step1 (* input-data 2)
              step2 (+ step1 10)
              step3 (* step2 step2)]
          (swap! results assoc :path :complex :steps [step1 step2 step3] :result step3))
        ; Path B: Simple processing  
        (let [result (+ input-data 1)]
          (swap! results assoc :path :simple :result result))))
    
    (Thread/sleep 50)
    @results))

^{::clerk/visibility {:result :show}}
{:large-number (conditional-flow-demo #(> % 10) 15)
 :small-number (conditional-flow-demo #(> % 10) 5)}

;; ### Flow Monitoring and Metrics

^{::clerk/visibility {:result :show}}
(defn monitored-flow [processing-steps input-data]
  "Flow execution with monitoring and metrics"
  (let [metrics (atom {:start-time (System/currentTimeMillis)
                       :steps []
                       :total-processing-time 0})
        results (atom [])]
    
    (go 
      (loop [data input-data
             remaining-steps processing-steps]
        (if-let [step (first remaining-steps)]
          (let [step-start (System/currentTimeMillis)
                step-result ((:fn step) data)
                step-end (System/currentTimeMillis)
                step-duration (- step-end step-start)]
            
            (swap! metrics update :steps conj 
                   {:id (:id step) 
                    :duration step-duration
                    :input data 
                    :output step-result})
            (swap! metrics update :total-processing-time + step-duration)
            (swap! results conj {:step (:id step) :result step-result})
            
            (recur step-result (rest remaining-steps)))
          
          (swap! metrics assoc :end-time (System/currentTimeMillis)))))
    
    (Thread/sleep 100)
    {:metrics @metrics
     :results @results}))

^{::clerk/visibility {:result :show}}
(let [steps [{:id :double :fn #(* % 2)}
             {:id :add-ten :fn #(+ % 10)}  
             {:id :square :fn #(* % %)}]]
  (monitored-flow steps 5))

;; ### Flow Composition

^{::clerk/visibility {:result :show}}
(defn compose-flows [flow1 flow2]
  "Composes two flows into a single flow"
  (fn [input]
    (go 
      (let [intermediate-result (<! (flow1 input))]
        (<! (flow2 intermediate-result))))))

^{::clerk/visibility {:result :show}}
(defn math-flow [x]
  (go 
    (<! (timeout 30))
    (* x x)))

^{::clerk/visibility {:result :show}}
(defn string-flow [x]
  (go 
    (<! (timeout 20))
    (str "Result: " x)))

^{::clerk/visibility {:result :show}}
(let [composed-flow (compose-flows math-flow string-flow)]
  (<!! (composed-flow 7)))

;; ## Summary

;; This notebook introduced flow orchestration concepts:

;; - **Flow Nodes**: Individual processing steps with dependencies
;; - **Linear Flows**: Sequential data processing
;; - **Parallel Flows**: Concurrent branch execution
;; - **Dependencies**: Complex dependency graphs  
;; - **Error Handling**: Graceful error propagation
;; - **State Management**: Maintaining state across execution
;; - **Conditional Execution**: Dynamic flow paths
;; - **Monitoring**: Metrics and performance tracking
;; - **Composition**: Building complex flows from simpler ones

;; These concepts provide the foundation for building sophisticated
;; data processing pipelines using core.async primitives.
;; Next, we'll explore more advanced flow orchestration patterns.