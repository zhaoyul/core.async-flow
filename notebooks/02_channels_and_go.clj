;; # Channels and Go Blocks Deep Dive
;; This notebook explores advanced channel patterns and go block usage.

(ns notebooks.02-channels-and-go
  (:require [nextjournal.clerk :as clerk]
            [clojure.core.async :as async :refer [go go-loop <! >! <!! >!! chan close! 
                                                  timeout alts! mult tap untap
                                                  pipe pipeline pipeline-blocking
                                                  merge mix admix unmix toggle]]
            [clojure.core.async.impl.protocols :as protocols]))

;; ## Advanced Channel Patterns

;; ### Producer-Consumer Pattern

;; The classic producer-consumer pattern with multiple producers and consumers:

^{::clerk/visibility {:result :show}}
(defn producer [ch name items delay-ms]
  (go
    (doseq [item items]
      (<! (timeout delay-ms))
      (>! ch {:producer name :item item :timestamp (System/currentTimeMillis)}))
    (println (str "Producer " name " finished"))))

^{::clerk/visibility {:result :show}}
(defn consumer [ch name]
  (go-loop [consumed []]
    (if-let [item (<! ch)]
      (do 
        (println (str "Consumer " name " got: " (:item item) " from " (:producer item)))
        (recur (conj consumed item)))
      consumed)))

^{::clerk/visibility {:result :show}}
(let [work-ch (chan 5) ; Buffered channel for work items
      results (atom {})]
  
  ;; Start multiple producers
  (producer work-ch "A" [1 2 3] 50)
  (producer work-ch "B" [4 5 6] 30)
  
  ;; Start multiple consumers
  (go (reset! results (merge @results {:consumer-1 (<! (consumer work-ch "1"))})))
  (go (swap! results assoc :consumer-2 (<! (consumer work-ch "2"))))
  
  ;; Close channel after all producers finish
  (go 
    (<! (timeout 500)) ; Wait for producers to finish
    (close! work-ch))
  
  (Thread/sleep 600)
  @results)

;; ### Fan-out Pattern (mult and tap)

;; Broadcasting messages to multiple consumers:

^{::clerk/visibility {:result :show}}
(let [source-ch (chan)
      mult-ch (mult source-ch)
      consumer1-ch (chan)
      consumer2-ch (chan)
      consumer3-ch (chan)
      results (atom {})]
  
  ;; Tap multiple channels to the mult
  (tap mult-ch consumer1-ch)
  (tap mult-ch consumer2-ch)
  (tap mult-ch consumer3-ch)
  
  ;; Start consumers
  (go (swap! results assoc :consumer-1 
             (<! (go-loop [items []]
                   (if-let [item (<! consumer1-ch)]
                     (recur (conj items (str "C1:" item)))
                     items)))))
  
  (go (swap! results assoc :consumer-2 
             (<! (go-loop [items []]
                   (if-let [item (<! consumer2-ch)]
                     (recur (conj items (str "C2:" item)))
                     items)))))
  
  (go (swap! results assoc :consumer-3 
             (<! (go-loop [items []]
                   (if-let [item (<! consumer3-ch)]
                     (recur (conj items (str "C3:" item)))
                     items)))))
  
  ;; Produce data
  (go 
    (doseq [i (range 5)]
      (>! source-ch (str "message-" i))
      (<! (timeout 20)))
    (close! source-ch))
  
  (Thread/sleep 300)
  @results)

;; ### Fan-in Pattern (merge)

;; Combining multiple input channels into one output channel:

^{::clerk/visibility {:result :show}}
(let [ch1 (chan)
      ch2 (chan) 
      ch3 (chan)
      merged-ch (merge [ch1 ch2 ch3])
      results (atom [])]
  
  ;; Producers for different channels
  (go 
    (doseq [i [1 3 5]]
      (<! (timeout (* i 10)))
      (>! ch1 {:source "ch1" :value i}))
    (close! ch1))
  
  (go 
    (doseq [i [2 4 6]]
      (<! (timeout (* i 15)))
      (>! ch2 {:source "ch2" :value i}))
    (close! ch2))
  
  (go 
    (doseq [i [7 8 9]]
      (<! (timeout (* i 5)))
      (>! ch3 {:source "ch3" :value i}))
    (close! ch3))
  
  ;; Consumer of merged channel
  (go-loop []
    (if-let [item (<! merged-ch)]
      (do 
        (swap! results conj item)
        (recur))
      (println "Merge complete")))
  
  (Thread/sleep 200)
  @results)

;; ### Pipeline Processing

;; Using pipeline for parallel processing:

^{::clerk/visibility {:result :show}}
(let [input-ch (chan)
      output-ch (chan)
      results (atom [])]
  
  ;; Create a pipeline that processes items in parallel
  (pipeline 3 ; parallelism of 3
            output-ch
            (map (fn [x] 
                   (Thread/sleep 50) ; Simulate work
                   (* x x)))
            input-ch)
  
  ;; Feed data into pipeline
  (go 
    (doseq [i (range 1 11)]
      (>! input-ch i))
    (close! input-ch))
  
  ;; Collect results
  (go-loop []
    (if-let [result (<! output-ch)]
      (do 
        (swap! results conj result)
        (recur))
      (println "Pipeline complete")))
  
  (Thread/sleep 300)
  (sort @results))

;; ### Blocking vs Non-blocking Operations

;; Demonstration of the difference between parking (`<!`) and blocking (`<!!`):

^{::clerk/visibility {:result :show}}
(let [results (atom {})]
  
  ;; Parking operations (inside go blocks)
  (let [ch (chan)]
    (go 
      (swap! results assoc :parking-put "Starting put")
      (>! ch "parking value")
      (swap! results assoc :parking-put "Put completed"))
    
    (go 
      (<! (timeout 50)) ; Small delay
      (swap! results assoc :parking-take "Starting take")
      (let [value (<! ch)]
        (swap! results assoc :parking-take (str "Took: " value)))))
  
  ;; Blocking operations (outside go blocks, use with caution!)
  (future 
    (let [ch (chan)]
      (future 
        (swap! results assoc :blocking-put "Starting blocking put")
        (>!! ch "blocking value")
        (swap! results assoc :blocking-put "Blocking put completed"))
      
      (Thread/sleep 50)
      (swap! results assoc :blocking-take "Starting blocking take")
      (let [value (<!! ch)]
        (swap! results assoc :blocking-take (str "Blocked and took: " value)))))
  
  (Thread/sleep 200)
  @results)

;; ### Channel Buffering Strategies

;; Different buffer types and their behaviors:

^{::clerk/visibility {:result :show}}
(defn test-buffer [buffer-type buffer-size]
  (let [ch (case buffer-type
             :fixed (chan buffer-size)
             :dropping (chan (async/dropping-buffer buffer-size))
             :sliding (chan (async/sliding-buffer buffer-size)))
        results (atom {:puts [] :takes []})]
    
    ;; Try to put more items than buffer size
    (go 
      (doseq [i (range 10)]
        (let [put-result (>! ch i)]
          (swap! results update :puts conj {:item i :success? put-result}))))
    
    ;; Take items
    (go 
      (<! (timeout 50)) ; Let puts complete first
      (loop []
        (when-let [item (<! ch)]
          (swap! results update :takes conj item)
          (recur))))
    
    (Thread/sleep 100)
    @results))

^{::clerk/visibility {:result :show}}
{:fixed-buffer (test-buffer :fixed 3)
 :dropping-buffer (test-buffer :dropping 3)
 :sliding-buffer (test-buffer :sliding 3)}

;; ### Error Propagation in Channels

;; Handling errors gracefully in channel-based systems:

^{::clerk/visibility {:result :show}}
(defn safe-processor [input-ch output-ch error-ch]
  (go-loop []
    (if-let [item (<! input-ch)]
      (try 
        (let [result (if (= item 0)
                       (throw (ex-info "Division by zero" {:item item}))
                       (/ 100 item))]
          (>! output-ch {:input item :result result}))
        (catch Exception e
          (>! error-ch {:error (.getMessage e) :data (ex-data e)})))
      (do 
        (close! output-ch)
        (close! error-ch)))
    (if (not (protocols/closed? input-ch))
      (recur))))

^{::clerk/visibility {:result :show}}
(let [input-ch (chan)
      output-ch (chan)
      error-ch (chan)
      results (atom {:outputs [] :errors []})]
  
  (safe-processor input-ch output-ch error-ch)
  
  ;; Collect outputs
  (go-loop []
    (if-let [output (<! output-ch)]
      (do 
        (swap! results update :outputs conj output)
        (recur))))
  
  ;; Collect errors
  (go-loop []
    (if-let [error (<! error-ch)]
      (do 
        (swap! results update :errors conj error)
        (recur))))
  
  ;; Send test data including problematic value
  (go 
    (doseq [i [10 5 2 0 1]] ; 0 will cause error
      (>! input-ch i))
    (close! input-ch))
  
  (Thread/sleep 100)
  @results)

;; ### Mix and Admix for Dynamic Channel Routing

;; Dynamic routing of channels using mix:

^{::clerk/visibility {:result :show}}
(let [input1 (chan)
      input2 (chan) 
      input3 (chan)
      output (chan)
      mixer (mix output)
      results (atom [])]
  
  ;; Initially admit only input1 and input2
  (admix mixer input1)
  (admix mixer input2)
  
  ;; Collect outputs
  (go-loop []
    (if-let [item (<! output)]
      (do 
        (swap! results conj item)
        (recur))))
  
  ;; Send data to all inputs
  (go (>! input1 "from-input1-1") (>! input1 "from-input1-2") (close! input1))
  (go (>! input2 "from-input2-1") (>! input2 "from-input2-2") (close! input2))
  (go (>! input3 "from-input3-1") (>! input3 "from-input3-2") (close! input3))
  
  (<! (timeout 50))
  
  ;; Now admit input3 and remove input1
  (unmix mixer input1)
  (admix mixer input3)
  
  ;; Send more data
  (go (>! input1 "from-input1-3")) ; This won't appear in output
  (go (>! input2 "from-input2-3"))
  (go (>! input3 "from-input3-3")) ; This will now appear
  
  (Thread/sleep 100)
  @results)

;; ## Summary

;; This notebook explored advanced core.async patterns:

;; - **Producer-Consumer**: Multiple producers and consumers working together
;; - **Fan-out**: Broadcasting with `mult` and `tap`
;; - **Fan-in**: Merging multiple channels with `merge`
;; - **Pipeline**: Parallel processing with `pipeline`
;; - **Blocking vs Parking**: Understanding `<!` vs `<!!`
;; - **Buffer Strategies**: Fixed, dropping, and sliding buffers
;; - **Error Handling**: Safe error propagation in async systems
;; - **Dynamic Routing**: Using `mix`, `admix`, and `unmix`

;; These patterns form the foundation for building robust asynchronous systems in Clojure.
;; Next, we'll look at more complex async patterns and real-world applications.