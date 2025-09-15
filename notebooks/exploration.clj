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
  (>!! a :a)
  (>!! b :b)
  [(<!! out) (<!! out)])

;; ## 3. flow 简单示例

(clerk/md "### 使用 flow 连接处理步骤")
(defn inc-step
  "将输入数字加一后发送到输出"
  ([] {:ins {:in "输入"} :outs {:out "输出"}})
  ([state port msg]
   [state {:out (inc msg)}]))

(defn sink-step
  "将所有输入累积到 state 中"
  ([] {:ins {:in "输入"}})
  ([state port msg]
   [(update state :seen conj msg) {}]))

(def inc-proc (flow/process inc-step))
(def sink-proc (flow/process sink-step))

(def g
  (flow/create-flow
    {:procs {::inc {:proc inc-proc}
             ::sink {:proc sink-proc}}
     :conns [[::start :out] [::inc :in]
             [::inc :out] [::sink :in]]}))

(flow/start g)
(flow/inject g [::start :out] [1 2 3])
(Thread/sleep 100)
(:seen (flow/ping-proc g ::sink))
(flow/stop g)

;; ## 4. 更复杂的 flow 拓扑

(clerk/md "### 同时广播到两个处理步骤")

(defn double-step
  "输入数字乘二"
  ([] {:ins {:in "输入"} :outs {:out "输出"}})
  ([state port msg]
   [state {:out (* 2 msg)}]))

(def double-proc (flow/process double-step))

(def g2
  (flow/create-flow
    {:procs {::inc   {:proc inc-proc}
             ::double {:proc double-proc}
             ::sink1 {:proc sink-proc}
             ::sink2 {:proc sink-proc}}
     :conns [[::start :out] [::inc :in]
             [::start :out] [::double :in]
             [::inc :out] [::sink1 :in]
             [::double :out] [::sink2 :in]]}))

(flow/start g2)
(flow/inject g2 [::start :out] (range 5))
(Thread/sleep 100)
{:inc (:seen (flow/ping-proc g2 ::sink1))
 :double (:seen (flow/ping-proc g2 ::sink2))}

;; ## 5. flow-monitor 监控

(clerk/md "### 启动监控服务器观察 flow 状态")
(def server (fmon/start-server {:flow g2 :port 9999}))
;; 访问 http://localhost:9999/index.html 查看实时状态
(fmon/stop-server server)
(flow/stop g2)

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
