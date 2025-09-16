;; # Clojure core.async/flow 全面体验笔记
^{:nextjournal.clerk/visibility {:code :hide}}
(ns exploration
  (:require [clojure.core.async :as a :refer [>! <! >!! <!! go go-loop chan buffer dropping-buffer sliding-buffer promise-chan put! take! close! onto-chan! alt!
                                              pub sub unsub mix admix unmix pipe mult tap untap timeout alts! alts!!]]
            [clojure.core.async.flow :as flow]
            [clojure.core.async.flow-monitor :as fmon]
            [nextjournal.clerk :as clerk]
            [nextjournal.clerk.viewer :as v])
  (:import [java.lang.management ManagementFactory]
           [java.lang Runtime]))


^{:nextjournal.clerk/visibility {:code :hide :result :hide}}
(clerk/add-viewers!
 [{:pred #(instance? clojure.core.async.impl.channels.ManyToManyChannel %)
   :render-fn '(fn [] [:h1.text-green-500 "🛣️"])}])


;; ## 0. 为什么有core.async

^{:nextjournal.clerk/visibility {:code :show}}
(defn get-memory-usage
  "查看当前jvm的堆内存, 返回一个map:
  {:total 536, :free 373, :used 163}"
  []
  (let [runtime (Runtime/getRuntime)]
    {:total (int (/ (.totalMemory runtime) 1e6))
     :free (int (/ (.freeMemory runtime) 1e6))
     :used (int (/ (- (.totalMemory runtime) (.freeMemory runtime)) 1e6))}))

^{:nextjournal.clerk/visibility {:code :show}}
(defn get-thread-count "获取总的线程数量" []
  (let [thread-mx-bean (ManagementFactory/getThreadMXBean)]
    (.getThreadCount thread-mx-bean)))


^{:nextjournal.clerk/visibility {:code :show :result :show}}
(clerk/table (let [m (get-memory-usage)]
               [(keys m)
                (vals m)]))


(get-thread-count)

^{:nextjournal.clerk/visibility {:code :show :result :show}}
(clerk/table (let [m (get-memory-usage)]
               [(keys m)
                (vals m)]))


;; **创建百万个channel**
^{:nextjournal.clerk/visibility {:code :show :result :show}}
(dotimes [_ 1e6]
  (go
    (<! (chan))))

^{:nextjournal.clerk/visibility {:code :show :result :show}}
(dotimes [_ 1e3]
  (future
    (Thread/sleep 100000)))



^{:nextjournal.clerk/visibility {:code :show :result :show}}
(clerk/table (let [m (get-memory-usage)]
               [(keys m)
                (vals m)]))


(get-thread-count)




;; ## 1. core.async 基础回顾

;; ### Go 块与 Channel
(def greeting-ch (chan))
(go (>! greeting-ch "你好, core.async")) ; 将消息放入 channel
(<!! greeting-ch)                       ; => "你好, core.async"

;; ### 缓冲区示例

(def drop-ch (chan (dropping-buffer 1)))
(>!! drop-ch :a)
(>!! drop-ch :b)
(<!! drop-ch) ;; 放不进去的:b会被drop掉

(def slide-ch (chan (sliding-buffer 1)))
(>!! slide-ch :a)
(>!! slide-ch :b)
(<!! slide-ch)

;; ### alts! 从多个 channel 读取
(let [c1 (chan)
      c2 (chan)]
  (go (<! (timeout 100)) (>! c1 :c1))
  (go (<! (timeout 50)) (>! c2 :c2))
  (<!! (go (alts! [c1 c2]))))

;; ## 2. 进阶 Channel 操作

;; ### 管道(pipe)
(let [in (chan)
      out (chan)]
  (pipe in out)
  (>!! in :hello)
  (<!! out))

;; ### 多路分发(mult/tap)
(let [source (chan)
      m (mult source)
      c1 (chan)
      c2 (chan)]
  (tap m c1)
  (tap m c2)
  (>!! source :hi)
  [(<!! c1) (<!! c2)])

;; ### 订阅(pub/sub)
(let [source (chan)
      p (pub source :topic)
      t1 (chan)
      t2 (chan)]
  (sub p :foo t1) ;; 从p中订阅 :foo 主题
  (sub p :bar t2) ;; 从p中订阅 :bar 主题
  (>!! source {:topic :foo :msg 1})
  (>!! source {:topic :bar :msg 2})
  [(<!! t1) (<!! t2)])

;; ### mix / admix
(let [a (chan)
      b (chan)
      out (chan)
      m (mix out)]
  (admix m a)
  (admix m b)
  (go (>!! a :a))
  (go (>!! b :b))
  [(<!! out) (<!! out)])

;; ### alt!
(let [chan-a (chan)
      chan-b (chan)
      chan-c (chan)
      chan-d (chan)
      chan-e (chan)
      chan-f (chan)
      chan-g (chan)
      chan-h (chan)
      chan-i (chan)]
  (go-loop [i 10]
    (when (> i 0)
      (println
       (alt!
         chan-a :receive-from-a               ;; 单个接收chan, 返回表达式
         chan-b ([v] (println "b:" v))        ;; 单个接收chan, 一个参数的callback func
         [chan-c chan-d] :receive-from-c-or-d ;; 多个接收chan, 返回表达式
         [chan-e chan-f] ([v c]               ;; 多个接收chan, 两个参数的call-back
                          (println "receive:" v "from" c))
         [[chan-g "v"]]  :send-to-g           ;; 单个发送chan,
         [[chan-h "h"]]  ([v]                 ;; 单个发送chan, 一个参数的call-back
                          (println "from h:" v))
         [[chan-i "i"]]  ([v c]               ;; 单个发送chan, 两个参数的call-back
                          (println "send:" v "from" c))
         ;;(timeout 100) :timeout
         :default 42
         )))
    (recur (dec i)))

  (put! chan-a "msg...") ;; return=> :receive-from-a
  (put! chan-b "msg...")   ;; print => b: msg...
  (put! chan-c "msg...")   ;; return=> :receive-from-c-or-d
  (put! chan-d "msg...")   ;; return=> :receive-from-c-or-d
  (put! chan-e "msg...")  ;; print=> receive: msg... from #object...
  (put! chan-f "msg...")  ;; print=> receive: msg... from #object...
  [(<!! chan-g)           ;; print=> :send-to-g
   (<!! chan-h)           ;; print=> from h: true
   (<!! chan-i)]           ;; print=> from h: true
  )

;; ### 关闭channel

;; 1. close! 是个副作用函数, 始终返回nil
;; 1. 从关闭的channel中, 取出的始终是nil
;; 2. 放入关闭的channel, 返回值始终是false
(let [c (chan)]
  [(close! c)
   (<!! c)
   (put! c 100)])


;; **注意**: 以前放入的值, 还可以拿到, 拿到之后永远是nil
(let [c (chan)]
  (put! c 100)
  [(close! c)
   (<!! c)   ;; 拿到100
   (<!! c)   ;; 拿到nil
   (put! c 100)])

;; 对pipe的影响
(let [in (chan)
      out (chan)]
  (pipe in out)
  (>!! in :hello)
  (close! in)
  [(<!! out)
   (<!! in)    ;; 上游in 被我们关闭
   (<!! out)]  ;; 下游out 会连带关闭
  )

;; 对多路分发的影响
(let [source (chan)
      m (mult source)
      c1 (chan)
      c2 (chan)]
  (tap m c1)
  (tap m c2)
  (>!! source :hi)
  [(<!! c1) (<!! c2)
   (close! source)  ;; 关闭source
   (<!! c1) (<!! c2)]) ;; 所有的tap都会关闭

;; 对pub/sub的影响
(let [source (chan)
      p (pub source :topic)
      t1 (chan)
      t2 (chan)]
  (sub p :foo t1) ;; 从p中订阅 :foo 主题
  (sub p :bar t2) ;; 从p中订阅 :bar 主题
  (>!! source {:topic :foo :msg 1})
  (>!! source {:topic :bar :msg 2})
  [(<!! t1) (<!! t2)
   (close! source)       ;; 关闭source
   (<!! t1) (<!! t2)])   ;; 所有的订阅都会关闭

;; ## 3. flow 简单示例

;; ### 使用 flow 连接处理步骤

(defn stat-gen
  "生成一个介于 min (包含) 和 max (不包含) 之间的随机值, 并将其写入 out 通道.
   在 stop-atom 被标记为 true 之前, 每次写入之间会等待 wait 毫秒. "
  ([out min max wait stop-atom]
   (loop []
     (let [val (+ min (rand-int (- max min)))
           put (a/>!! out val)]
       (println "stat-gen" (System/identityHashCode stop-atom) val put (not @stop-atom))
       (when (and put (not @stop-atom))
         (^[long] Thread/sleep wait)
         (recur))))))

(defn source
  "随机数生成"
  ;; 描述
  ([] {:params {:min "生成下限"
                :max "生成上限"
                :wait "毫秒间隔"}
       :outs {:out "随机数的输出channel"}})

  ;; 初始化状态, 状态map是状态切换的参数
  ([args]
   (assoc args
          ::flow/in-ports {:stat (a/chan 100)}
          :stop (atom false)))

  ;; 状态切换
  ([{:keys [min max wait ::flow/in-ports] :as state} transition]
   (println "transition" transition)
   (case transition
     ::flow/resume
     (let [stop-atom (atom false)]
       (future (stat-gen (:stat in-ports) min max wait stop-atom))
       (assoc state :stop stop-atom))

     (::flow/pause ::flow/stop)
     (do
       (reset! (:stop state) true)
       state)))

  ;; 数据处理
  ([state in msg]
   (println "source transform" in msg)
   [state (when (= in :stat) {:out [msg]})]))

(defn aggregator
  ;; 描述
  ([] {:params {:min "告警阈值下限, 低于则告警"
                :max "告警阈值上限限, 高于则告警"}
       :ins {:stat "收取数据的channel"
             :poke "触发系统日志的channel"}
       :outs {:alert "告警channel {:val value, :error :high|:low"}
       :workload :compute
       })

  ;; 初始化
  ([args] (assoc args :vals []))

  ;; 状态转化
  ([state transition] state)

  ;; 数据处理
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
  ;; 测试 aggregator 告警
  (let [state {:min 1 :max 5 :vals []}
        [state' msgs'] (aggregator state :stat 100)]
    (assert (= msgs' {:alert [{:val 100, :error :high}]})))
  )


(defn scheduler
  ;; 描述
  ([] {:params {:wait "收集系统数据的时间间隔"}
       :outs {:out "收集触发channel"}})

  ;; 初始化
  ([args]
   (assoc args
          ::flow/in-ports {:alarm (a/chan 10)}
          :stop (atom false)))

  ;; 状态转化
  ([{:keys [wait ::flow/in-ports] :as state} transition]
   (println "scheduler transition" transition state transition)
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

  ;; 数据处理
  ([state in msg]
   [state (when (= in :alarm) {:out [true]})]))

(defn printer
  ;; 描述
  ([] {:params {:prefix "Log message prefix"}
       :ins {:in "Channel to receive messages"}})

  ;; 初始化
  ([state] state)

  ;; 状态
  ([state _transition] state)

  ;; 数据处理
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
