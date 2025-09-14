;; # Core.async Basics
;; This notebook introduces the fundamental concepts of Clojure's core.async library.

(ns notebooks.01-core-async-basics
  (:require [nextjournal.clerk :as clerk]
            [clojure.core.async :as async :refer [go go-loop <! >! <!! >!! chan close! timeout alts!]]
            [clojure.core.async.impl.protocols :as protocols]))

;; ## Introduction to Core.async

;; Core.async brings Communicating Sequential Processes (CSP) to Clojure.
;; It provides channels for communication and go blocks for lightweight concurrency.

;; ### What are Channels?

;; Channels are first-class values that can be passed around, stored in data structures,
;; and used for communication between different parts of your program.

^{::clerk/visibility {:result :show}}
(comment
  "Channels are like queues that can be used to coordinate between different processes")

;; ### Creating Channels

;; The most basic operation is creating a channel:

^{::clerk/visibility {:result :show}}
(def basic-channel (chan))

;; Channels can have buffers to allow asynchronous puts:

^{::clerk/visibility {:result :show}}
(def buffered-channel (chan 10))

;; ### Basic Channel Operations

;; #### Putting Values onto Channels

;; We can put values onto channels using `>!` (blocking) or `>!!` (parking):

^{::clerk/visibility {:result :show}}
(let [ch (chan)]
  (go (>! ch "Hello, World!"))
  (go (println "Received:" (<! ch))))

;; #### Taking Values from Channels

;; Let's demonstrate taking values:

^{::clerk/visibility {:result :show}}
(let [ch (chan)
      result (atom [])]
  (go 
    (>! ch 1)
    (>! ch 2) 
    (>! ch 3)
    (close! ch))
  (go-loop []
    (when-let [value (<! ch)]
      (swap! result conj value)
      (recur)))
  ;; Give some time for async operations
  (Thread/sleep 100)
  @result)

;; ### Go Blocks

;; Go blocks are lightweight threads that can park (pause) without blocking OS threads.
;; They're the primary way to work with channels asynchronously.

^{::clerk/visibility {:result :show}}
(def demo-results (atom []))

^{::clerk/visibility {:result :show}}
(let [ch (chan)]
  (go
    (swap! demo-results conj "Producer starting...")
    (doseq [i (range 5)]
      (>! ch i)
      (swap! demo-results conj (str "Produced: " i)))
    (close! ch)
    (swap! demo-results conj "Producer finished"))
  
  (go
    (swap! demo-results conj "Consumer starting...")
    (loop []
      (when-let [value (<! ch)]
        (swap! demo-results conj (str "Consumed: " value))
        (recur)))
    (swap! demo-results conj "Consumer finished"))
  
  ;; Wait for completion
  (Thread/sleep 100)
  @demo-results)

;; ### Channel Transformations

;; Channels support transducers for data transformation:

^{::clerk/visibility {:result :show}}
(let [input-ch (chan)
      output-ch (chan 10 (map #(* % % %)))] ; Transform to cube
  (async/pipe input-ch output-ch)
  
  (go 
    (doseq [i (range 1 6)]
      (>! input-ch i))
    (close! input-ch))
  
  (go-loop [results []]
    (if-let [value (<! output-ch)]
      (recur (conj results value))
      results)))

;; ### Timeout Channels

;; Timeout channels automatically close after a specified time:

^{::clerk/visibility {:result :show}}
(let [timeout-demo (atom [])]
  (go
    (swap! timeout-demo conj "Starting timeout demo...")
    (let [timeout-ch (timeout 100)] ; 100ms timeout
      (swap! timeout-demo conj "Waiting...")
      (<! timeout-ch)
      (swap! timeout-demo conj "Timeout reached!")))
  
  (Thread/sleep 150)
  @timeout-demo)

;; ### Alts! - Channel Selection

;; `alts!` allows you to wait on multiple channels and take from whichever is ready first:

^{::clerk/visibility {:result :show}}
(let [ch1 (chan)
      ch2 (chan)
      results (atom [])]
  
  (go 
    (<! (timeout 50))
    (>! ch1 "Fast channel"))
  
  (go 
    (<! (timeout 100))
    (>! ch2 "Slow channel"))
  
  (go
    (let [[value channel] (alts! [ch1 ch2])]
      (swap! results conj 
             {:value value 
              :channel (cond 
                        (= channel ch1) "ch1"
                        (= channel ch2) "ch2"
                        :else "unknown")})))
  
  (Thread/sleep 150)
  @results)

;; ### Error Handling

;; Channels can carry any values, including exceptions:

^{::clerk/visibility {:result :show}}
(let [error-ch (chan)
      result (atom nil)]
  (go
    (try 
      (>! error-ch (/ 1 0)) ; This will throw
      (catch Exception e
        (>! error-ch {:error (.getMessage e)}))))
  
  (go 
    (let [value (<! error-ch)]
      (reset! result value)))
  
  (Thread/sleep 50)
  @result)

;; ### Closing Channels

;; Properly closing channels is important for resource management:

^{::clerk/visibility {:result :show}}
(let [ch (chan)
      status (atom {})]
  (go 
    (swap! status assoc :open? (not (protocols/closed? ch)))
    (close! ch)
    (swap! status assoc :after-close? (protocols/closed? ch)))
  
  (Thread/sleep 50)
  @status)

;; ## Summary

;; This notebook covered the fundamental concepts of core.async:
;; 
;; - **Channels**: First-class communication primitives
;; - **Go blocks**: Lightweight concurrent processes  
;; - **Channel operations**: `>!`, `<!`, `close!`
;; - **Transformations**: Using transducers with channels
;; - **Timeouts**: Automatic channel closing
;; - **Selection**: `alts!` for waiting on multiple channels
;; - **Error handling**: Channels can carry any values
;; - **Resource management**: Properly closing channels
;;
;; In the next notebook, we'll explore more advanced channel patterns and real-world use cases.