(ns radar.server
  (:refer-clojure :exclude [send])
  (:use [link core tcp])
  (:use [radar codec])
  (:import [java.net InetSocketAddress])
  (:import [org.apache.commons.pool PoolableObjectFactory])
  (:import [org.apache.commons.pool.impl GenericObjectPool]))


(def south-connection-pools (atom {}))

(defn init-session [key]
  (.borrowObject
   ^GenericObjectPool (first (vals @south-connection-pools))))

(defn finish-session [session]
  (let [addr (remote-addr (:south-channel session))
        addr-key (str (.getHostName ^InetSocketAddress addr)
                      ":" (.getPort ^InetSocketAddress addr))
        pool (get @south-connection-pools addr-key)]
    (.returnObject ^GenericObjectPool pool session)))

(defn create-south-gate-handler [north-channel-ref]
  (create-handler
   (on-message [ch msg addr]
               (send @north-channel-ref msg)
               (reset! north-channel-ref nil)
               (finish-session {:north-channel north-channel-ref
                                :south-channel ch}))
   (on-error [ch e]
             (.printStackTrace e))))

(defn create-south-channel [host port]
  (let [upstream-channel-ref (atom nil)
        handler (create-south-gate-handler upstream-channel-ref)]
    {:south-channel (tcp-client host port handler
                                :encoder (redis-request-frame)
                                :decoder (redis-response-frame))
     :north-channel upstream-channel-ref}))

(defn pool-factory [host port]
  (reify
    PoolableObjectFactory
    (destroyObject [this obj]
      (close obj))
    (makeObject [this]
      (create-south-channel host port))
    (validateObject [this obj]
      (valid? obj))
    (activateObject [this obj]
      )
    (passivateObject [this obj]
      )))

(defn create-south-connection-pool [host port]
  (GenericObjectPool. (pool-factory host port) 8
                      GenericObjectPool/WHEN_EXHAUSTED_GROW -1))



(defn add-south-redis [host port]
  (swap! south-connection-pools assoc
         (str host ":" port)
         (create-south-connection-pool host port)))

(def north-gate-handler
  (create-handler
   (on-message [ch msg addr]
               (let [session (init-session (:key msg))]
                 (reset! (:north-channel session) ch)
                 (send (:south-channel session) (:packet msg))))
   (on-error [ch e]
             (.printStackTrace e))))

(defn start-server [port]
  (tcp-server port north-gate-handler
              :decoder (redis-request-frame)
              :encoder (redis-response-frame)
              :threaded? true
              :ordered? true))

