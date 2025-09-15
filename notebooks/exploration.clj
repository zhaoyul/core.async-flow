;; # Clojure core.async/flow 全面体验笔记
^{:nextjournal.clerk/visibility {:code :hide}}
(ns exploration
  (:require [clojure.core.async :as a :refer [>! <! >!! <!! go go-loop chan buffer dropping-buffer sliding-buffer promise-chan put! take! close! onto-chan!
                                              pub sub unsub mix admix unmix pipe mult tap untap timeout alts! alts!!]]
            [clojure.core.async.flow :as flow]
            [clojure.core.async.flow-monitor :as fmon]
            [nextjournal.clerk :as clerk]
            [nextjournal.clerk.viewer :as v]))

;; --- Clerk 设置 ---
;; ^{:nextjournal.clerk/visibility {:code :hide :result :hide}}
;; (clerk/set-viewers!
;;   [{:pred #(instance? clojure.core.async.impl.channels.ManyToManyChannel %)
;;     :render-fn '#(v/html [:div.text-gray-500 "[core.async channel]"])}])

;; 现在，我们开始探索吧！
;; (从这里开始撰写您的笔记和代码)

;; ## 1. core.async 基础回顾

(clerk/md "### Go 块与 Channel")
(def greeting-ch (chan))
(go (>! greeting-ch "你好，core.async")) ; 将消息放入 channel
(<!! greeting-ch)                       ; => "你好，core.async"

(clerk/md "### 缓冲区示例")
(def drop-ch (chan (dropping-buffer 1)))
(>!! drop-ch :a)
(>!! drop-ch :b) ; :a 被丢弃
(<!! drop-ch)    ; => :b

(def slide-ch (chan (sliding-buffer 1)))
(>!! slide-ch :a)
(>!! slide-ch :b) ; :a 被覆盖
(<!! slide-ch)    ; => :b

(clerk/md "### alts! 从多个 channel 读取")
(let [c1 (chan)
      c2 (chan)]
  (go (<! (timeout 100)) (>! c1 :c1))
  (go (<! (timeout 50)) (>! c2 :c2))
  (<!! (go (alts! [c1 c2]))))

;; ## 2. 进阶 Channel 操作

(clerk/md "### pipe")
(let [in (chan)
      out (chan)]
  (pipe in out)
  (>!! in :hello)
  (<!! out))

(clerk/md "### mult / tap")
(let [source (chan)
      m (mult source)
      c1 (chan)
      c2 (chan)]
  (tap m c1)
  (tap m c2)
  (>!! source :hi)
  [(<!! c1) (<!! c2)])

(clerk/md "### pub / sub")
(let [source (chan)
      p (pub source :topic)
      t1 (chan)
      t2 (chan)]
  (sub p :foo t1)
  (sub p :bar t2)
  (>!! source {:topic :foo :msg 1})
  (>!! source {:topic :bar :msg 2})
  [(<!! t1) (<!! t2)])

(clerk/md "### mix / admix")
(let [a (chan)
      b (chan)
      out (chan)
      m (mix out)]
  (admix m a)
  (admix m b)
  (go (>!! a :a))
  (go (>!! b :b))
  [(<!! out) (<!! out)])

;; ## 3. flow 简单示例

(clerk/md "### 使用 flow 连接处理步骤")

(defn stat-gen
  "Generates a random value between min (inclusive) and max (exclusive)
  and writes it to out chan, waiting wait ms between until stop-atom is flagged."
  ([out min max wait stop-atom]
   (loop []
     (let [val (+ min (rand-int (- max min)))
           put (a/>!! out val)]
                                        ;(println "stat-gen" (System/identityHashCode stop-atom) val put (not @stop-atom))
       (when (and put (not @stop-atom))
         (^[long] Thread/sleep wait)
         (recur))))))

(defn source
  "Source proc for random stats"
  ;; describe
  ([] {:params {:min "Min value to generate"
                :max "Max value to generate"
                :wait "Time in ms to wait between generating"}
       :outs {:out "Output channel for stats"}})

  ;; init
  ([args]
   (assoc args
          ::flow/in-ports {:stat (a/chan 100)}
          :stop (atom false)))

  ;; transition
  ([{:keys [min max wait ::flow/in-ports] :as state} transition]
                                        ;(println "transition" transition)
   (case transition
     ::flow/resume
     (let [stop-atom (atom false)]
       (future (stat-gen (:stat in-ports) min max wait stop-atom))
       (assoc state :stop stop-atom))

     (::flow/pause ::flow/stop)
     (do
       (reset! (:stop state) true)
       state)))

  ;; transform
  ([state in msg]
                                        ;(println "source transform" in msg)
   [state (when (= in :stat) {:out [msg]})]))

(defn aggregator
  ;; describe
  ([] {:params {:min "Min value, alert if lower"
                :max "Max value, alert if higher"}
       :ins {:stat "Channel to receive stat values"
             :poke "Channel to poke when it is time to report a window of data to the log"}
       :outs {:alert "Notify of value out of range {:val value, :error :high|:low"}
       :workload :compute
       })

  ;; init
  ([args] (assoc args :vals []))

  ;; transition
  ([state transition] state)

  ;; transform
  ([{:keys [min max vals] :as state} input-id msg]
   (case input-id
     :stat (let [state' (assoc state :vals (conj vals msg))
                 msgs (cond
                        (< msg min) {:alert [{:val msg, :error :low}]}
                        (< max msg) {:alert [{:val msg, :error :high}]}
                        :else nil)]
             [state' msgs])
     :poke [(assoc state :vals [])
            {::flow/report (if (empty? vals)
                             [{:count 0}]
                             [{:avg (/ (double (reduce + vals)) (count vals))
                               :count (count vals)}])}]
     [state nil])))

(comment
  ;; test aggregator alert case - no channels involved
  (let [state {:min 1 :max 5 :vals []}
        [state' msgs'] (aggregator state :stat 100)]
    (assert (= msgs' {:alert [{:val 100, :error :high}]})))
  )


(defn scheduler
  ;; describe
  ([] {:params {:wait "Time to wait between pokes"}
       :outs {:out "Poke channel, will send true when the alarm goes off"}})

  ;; init
  ([args]
   (assoc args
          ::flow/in-ports {:alarm (a/chan 10)}
          :stop (atom false)))

  ;; transition
  ([{:keys [wait ::flow/in-ports] :as state} transition]
                                        ;(println "scheduler transition" transition state transition)
   (case transition
     ::flow/resume
     (let [stop-atom (atom false)]
       (future (loop []
                 (let [put (a/>!! (:alarm in-ports) true)]
                   (when (and put (not @stop-atom))
                     (^[long] Thread/sleep wait)
                     (recur)))))
       (assoc state :stop stop-atom))

     (::flow/pause ::flow/stop)
     (do
       (reset! (:stop state) true)
       state)))

  ;; transform
  ([state in msg]
   [state (when (= in :alarm) {:out [true]})]))

(defn printer
  ;; describe
  ([] {:params {:prefix "Log message prefix"}
       :ins {:in "Channel to receive messages"}})

  ;; init
  ([state] state)

  ;; transition
  ([state _transition] state)

  ;; transform
  ([{:keys [prefix] :as state} _in msg]
   (println prefix msg)
   [state nil]))

(defn create-flow
  []
  (flow/create-flow
   {:procs {:generator {:args {:min 0 :max 12 :wait 500} :proc (flow/process #'source)}
            :aggregator {:args {:min 1 :max 10} :proc (flow/process #'aggregator)}
            :scheduler {:args {:wait 3000} :proc (flow/process #'scheduler)}
            :notifier {:args {:prefix "Alert: "} :proc (flow/process #'printer)
                       :chan-opts {:in {:buf-or-n (a/sliding-buffer 3)}}}}
    :conns [[[:generator :out] [:aggregator :stat]]
            [[:scheduler :out] [:aggregator :poke]]
            [[:aggregator :alert] [:notifier :in]]]}))

(comment
  (def f (create-flow))
  (def chs (flow/start f))
  (flow/resume f)
  (flow/pause f)
  (flow/stop f)

  (def server (fmon/start-server {:flow f}))
  (fmon/stop-server server)

  @(flow/inject f [:aggregator :poke] [true])
  @(flow/inject f [:aggregator :stat] ["abc1000"]) ;; trigger an alert
  @(flow/inject f [:notifier :in] [:sandwich])

  (def report-chan (:report-chan chs))
  (flow/ping f)
  (a/poll! report-chan)
  (def error-chan (:error-chan chs))
  (a/poll! error-chan)

  (flow/stop f)
  (a/close! stat-chan)

  @(flow/inject f [:aggregator :poke] [true])

  (require '[clojure.datafy :as datafy])
  (datafy/datafy f)

  (require '[clojure.core.async.flow-static :refer [graph]])
  (graph f)

  )
(comment

  ;; start Clerk's built-in webserver on the default port 7777, opening the browser when done
  (clerk/serve! {:browse true})

  ;; either call `clerk/show!` explicitly
  (clerk/show! "notebooks/exploration.clj")

  ;; or let Clerk watch the given `:paths` for changes
  (clerk/serve! {:watch-paths ["notebooks" "src"]})

  ;; start with watcher and show filter function to enable notebook pinning
  (clerk/serve! {:watch-paths ["notebooks" "src"] :show-filter-fn #(clojure.string/starts-with? % "notebooks")})

  ;; Build a html file from the given notebook notebooks.
  ;; See the docstring for more options.
  (clerk/build! {:paths ["notebooks/rule_30.clj"]})

  )
