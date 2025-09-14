(ns core-async-flow.visualization
  (:require [clojure.core.async :as async :refer [go go-loop <! >! chan close! timeout]]
            [nextjournal.clerk :as clerk]))

;; Visualization helpers for Clerk notebooks

(defn flow-graph-viz [nodes edges]
  "Creates a simple flow graph visualization"
  {:type :flow-graph
   :nodes (map (fn [node]
                 {:id (:id node)
                  :label (or (:label node) (name (:id node)))
                  :type (or (:type node) :process)})
               nodes)
   :edges (map (fn [edge]
                 {:from (:from edge)
                  :to (:to edge)
                  :label (or (:label edge) "")})
               edges)})

(defn timeline-viz [events]
  "Creates a timeline visualization of events"
  {:type :timeline
   :events (map (fn [event]
                  {:timestamp (:timestamp event)
                   :label (:label event)
                   :type (or (:type event) :info)
                   :data (:data event)})
                events)})

(defn metrics-viz [metrics]
  "Creates a metrics visualization"
  {:type :metrics
   :data (map (fn [[key value]]
                {:metric (name key)
                 :value value
                 :type (cond 
                        (number? value) :numeric
                        (string? value) :text
                        :else :object)})
              metrics)})

(defn channel-flow-diagram [channel-info]
  "Creates a diagram showing channel flow"
  {:type :channel-flow
   :channels (map (fn [ch-info]
                    {:name (:name ch-info)
                     :buffer-size (:buffer-size ch-info 0)
                     :current-items (:current-items ch-info 0)
                     :producers (:producers ch-info [])
                     :consumers (:consumers ch-info [])})
                  channel-info)})

(defn performance-chart [performance-data]
  "Creates a performance chart"
  {:type :performance
   :series (map (fn [series]
                  {:name (:name series)
                   :data (:data series)
                   :unit (or (:unit series) "ms")})
                performance-data)})

(defn async-pattern-viz [pattern-type steps]
  "Visualizes common async patterns"
  (case pattern-type
    :producer-consumer
    {:type :producer-consumer
     :producers (:producers steps [])
     :consumers (:consumers steps [])
     :queue (:queue steps)}
    
    :fan-out
    {:type :fan-out
     :source (:source steps)
     :destinations (:destinations steps [])}
    
    :fan-in  
    {:type :fan-in
     :sources (:sources steps [])
     :destination (:destination steps)}
    
    :pipeline
    {:type :pipeline
     :stages (:stages steps [])}
    
    {:type :generic
     :steps steps}))

(defn error-analysis-viz [errors]
  "Creates visualization for error analysis"
  {:type :error-analysis
   :total-errors (count errors)
   :error-types (frequencies (map :type errors))
   :error-timeline (map #(select-keys % [:timestamp :type :message]) errors)
   :error-sources (frequencies (map :source errors))})

(defn throughput-viz [throughput-data time-window]
  "Creates throughput visualization"
  {:type :throughput
   :time-window time-window
   :data (map (fn [point]
                {:timestamp (:timestamp point)
                 :throughput (:count point)
                 :unit "ops/sec"})
              throughput-data)})

(defn dependency-graph-viz [dependencies]
  "Creates a dependency graph visualization"
  {:type :dependency-graph
   :nodes (keys dependencies)
   :edges (mapcat (fn [[node deps]]
                    (map #(hash-map :from % :to node) deps))
                  dependencies)})

(defn state-machine-viz [states transitions current-state]
  "Creates state machine visualization"
  {:type :state-machine
   :states (map (fn [state]
                  {:id state
                   :label (name state)
                   :current (= state current-state)})
                states)
   :transitions (map (fn [transition]
                       {:from (:from transition)
                        :to (:to transition)
                        :trigger (:trigger transition)})
                     transitions)})

;; Helper functions for collecting visualization data

(defn collect-channel-metrics [channels duration-ms]
  "Collects metrics from multiple channels over time"
  (let [metrics (atom [])]
    (go-loop [start-time (System/currentTimeMillis)]
      (when (< (- (System/currentTimeMillis) start-time) duration-ms)
        (let [current-metrics (map (fn [ch]
                                     {:name (:name ch)
                                      :timestamp (System/currentTimeMillis)
                                      :buffer-count (async/offer! (:channel ch) ::test-item)}) ; Hacky way to check buffer
                                   channels)]
          (swap! metrics conj current-metrics))
        (<! (timeout 100))
        (recur start-time)))
    @metrics))

(defn trace-execution [operation]
  "Traces the execution of an async operation"
  (let [trace (atom [])]
    (go 
      (swap! trace conj {:event :start :timestamp (System/currentTimeMillis)})
      (let [result (<! (operation))]
        (swap! trace conj {:event :complete :timestamp (System/currentTimeMillis) :result result})
        {:result result :trace @trace}))))

(defn monitor-flow-performance [flow-fn input-data sample-size]
  "Monitors flow performance over multiple runs"
  (go 
    (let [results (atom [])]
      (doseq [i (range sample-size)]
        (let [start-time (System/currentTimeMillis)
              result (<! (flow-fn input-data))
              end-time (System/currentTimeMillis)
              duration (- end-time start-time)]
          (swap! results conj {:run i 
                               :duration duration 
                               :result result
                               :timestamp start-time})))
      @results)))

;; Integration with Clerk

^{::clerk/visibility {:code :hide :result :show}}
(defn display-flow-viz [viz-data]
  "Displays visualization in Clerk"
  (clerk/html 
    [:div 
     [:h3 "Flow Visualization"]
     [:pre (with-out-str (clojure.pprint/pprint viz-data))]]))

^{::clerk/visibility {:code :hide :result :show}}
(defn display-metrics-table [metrics]
  "Displays metrics as a table in Clerk"
  (clerk/table 
    (map (fn [[k v]]
           {:metric (name k) 
            :value v
            :type (type v)})
         metrics)))

^{::clerk/visibility {:code :hide :result :show}}
(defn display-timeline [events]
  "Displays timeline of events"
  (clerk/table
    (map (fn [event]
           {:timestamp (java.time.Instant/ofEpochMilli (:timestamp event))
            :event (:event event)
            :data (:data event)})
         events)))