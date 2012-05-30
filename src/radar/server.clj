(ns radar.server
  (:refer-clojure :exclude [send])
  (:use [link core tcp pool])
  (:use [radar codec])
  (:import [java.net InetSocketAddress])
  (:import [org.apache.commons.pool PoolableObjectFactory])
  (:import [org.apache.commons.pool.impl GenericObjectPool]))

(def tcp-options
  {"child.reuseAddress" true,
   "reuseAddress" true,
   "child.keepAlive" true,
   "child.connectTimeoutMillis" 100,
   "tcpNoDelay" true,
   "readWriteFair" true,
   "child.tcpNoDelay" true,
   "receiveBufferSize" (* 64 1024)})

(defonce south-connection-pools (atom {}))

(defonce session-context (atom {}))

(defn init-session [key north-ch]
  (let [south-ch (borrow (first (vals @south-connection-pools)))]
    (swap! session-context assoc
           (channel-addr south-ch) {:north-channel north-ch})
    {:south-channel south-ch
     :north-channel north-ch}))

(defn finish-session [session]
  (let [south-ch (:south-channel session)
        addr (remote-addr south-ch)
        addr-key (str (.getHostName ^InetSocketAddress addr)
                      ":" (.getPort ^InetSocketAddress addr))
        pool (get @south-connection-pools addr-key)]
    (swap! session-context dissoc (channel-addr south-ch))
    (return pool (:south-channel session))))

(def south-gate-handler
  (create-handler
   (on-message [ch msg addr]
               (let [session (get @session-context (channel-addr ch))]
                 (send (:north-channel session) msg)
                 (finish-session {:south-channel ch})))
   (on-error [ch e]
             (.printStackTrace e)
             (finish-session {:south-channel ch})
             (close ch))))

(defn create-south-connection-pool [host port]
  (tcp-client-pool host port south-gate-handler
                   :encoder (redis-request-frame)
                   :decoder (redis-response-frame)))

(defn add-south-redis [host port]
  (swap! south-connection-pools assoc
         (str host ":" port)
         (create-south-connection-pool host port)))

(def north-gate-handler
  (create-handler
   (on-message [ch msg addr]
               (let [session (init-session (:key msg) ch)]
                 (send (:south-channel session) (:packet msg))))
   (on-error [ch e]
             (.printStackTrace e)
             (close ch))))

(defn start-server [port]
  (tcp-server port north-gate-handler
              :decoder (redis-request-frame)
              :encoder (redis-response-frame)
              :threaded? true
              :ordered? false
              :tcp-options tcp-options))

