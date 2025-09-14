(ns user
  (:require [nextjournal.clerk :as clerk]
            [clojure.core.async :as async]
            [core-async-flow.examples :as examples]
            [core-async-flow.utils :as utils]
            [core-async-flow.visualization :as viz]))

;; Development utilities and REPL helpers

(comment
  ;; Start Clerk server
  (clerk/serve! {:browse? true :port 7777})
  
  ;; Stop Clerk server
  (clerk/halt!)
  
  ;; Show specific notebook
  (clerk/show! "notebooks/01_core_async_basics.clj")
  (clerk/show! "notebooks/02_channels_and_go.clj")
  (clerk/show! "notebooks/03_async_patterns.clj")
  (clerk/show! "notebooks/04_flow_basics.clj")
  
  ;; Build all notebooks
  (clerk/build! {:paths ["notebooks"]
                 :out-path "docs"})
  
  ;; Quick test of core.async
  (let [ch (async/chan)]
    (async/go (async/>! ch "Hello from REPL"))
    (async/<!! ch))
  
  ;; Test examples
  (examples/create-producer (async/chan) "test" [1 2 3] 100)
  
  ;; Test utilities
  (utils/now)
  (async/<!! (utils/sleep-async 100))
  )

;; Helper functions for development

(defn quick-demo []
  "Quick demonstration of core.async basics"
  (let [ch (async/chan)
        results (atom [])]
    
    (async/go 
      (async/>! ch 1)
      (async/>! ch 2)
      (async/>! ch 3)
      (async/close! ch))
    
    (async/go-loop []
      (if-let [value (async/<! ch)]
        (do 
          (swap! results conj value)
          (recur))))
    
    (Thread/sleep 100)
    @results))

(defn benchmark-channels [n]
  "Simple benchmark of channel operations"
  (let [ch (async/chan)
        start-time (System/currentTimeMillis)]
    
    (async/go 
      (dotimes [i n]
        (async/>! ch i))
      (async/close! ch))
    
    (async/go 
      (loop [count 0]
        (if-let [value (async/<! ch)]
          (recur (inc count))
          (let [end-time (System/currentTimeMillis)
                duration (- end-time start-time)]
            (println (format "Processed %d items in %dms (%.2f ops/ms)" 
                           count duration (/ count (double duration))))))))
    
    nil))

(defn flow-demo []
  "Demonstrates a simple flow pattern"
  (let [input-ch (async/chan)
        output-ch (async/chan)
        results (atom [])]
    
    ;; Processing pipeline
    (async/go-loop []
      (if-let [value (async/<! input-ch)]
        (let [processed (* value value)]
          (async/>! output-ch processed)
          (recur))
        (async/close! output-ch)))
    
    ;; Result collector
    (async/go-loop []
      (if-let [result (async/<! output-ch)]
        (do 
          (swap! results conj result)
          (recur))))
    
    ;; Feed data
    (async/go 
      (doseq [i (range 1 6)]
        (async/>! input-ch i))
      (async/close! input-ch))
    
    (Thread/sleep 100)
    @results))

;; Visualization helpers

(defn viz-demo []
  "Demonstrates visualization functions"
  (viz/flow-graph-viz 
    [{:id :input :label "Input"} 
     {:id :process :label "Process"}
     {:id :output :label "Output"}]
    [{:from :input :to :process} 
     {:from :process :to :output}]))

;; Error testing

(defn error-demo []
  "Demonstrates error handling patterns"
  (let [ch (async/chan)
        error-ch (async/chan)
        results (atom [])]
    
    (async/go 
      (doseq [i [1 2 0 4 5]] ; 0 will cause division by zero
        (try 
          (let [result (/ 10 i)]
            (async/>! ch {:input i :result result}))
          (catch Exception e
            (async/>! error-ch {:input i :error (.getMessage e)})))))
    
    (async/close! ch)
    (async/close! error-ch)
    
    ;; Collect results
    (async/go-loop []
      (if-let [result (async/<! ch)]
        (do 
          (swap! results conj result)
          (recur))))
    
    (async/go-loop []
      (if-let [error (async/<! error-ch)]
        (do 
          (swap! results conj error)
          (recur))))
    
    (Thread/sleep 100)
    @results))

(println "Development environment loaded!")
(println "Try: (quick-demo), (flow-demo), (viz-demo), (error-demo)")
(println "To start Clerk: (clerk/serve! {:browse? true :port 7777})")