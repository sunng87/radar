(ns radar.server
  (:refer-clojure :exclude [send])
  (:use [clojure.string :only [split]])  
  (:use [link core tcp pool])
  (:use [radar codec])
  (:use [radar.config :only [conf]])
  (:import [java.net InetSocketAddress])
  (:import [clojure.lang PersistentQueue]))

(def tcp-options
  {"child.reuseAddress" true,
   "reuseAddress" true,
   "child.keepAlive" true,
   "child.connectTimeoutMillis" 100,
   "tcpNoDelay" true,
   "readWriteFair" true,
   "child.tcpNoDelay" true,
   "receiveBufferSize" (* 64 1024)})

(defonce south-connections (atom {}))

(defn host:port [^InetSocketAddress addr]
  (str (.getHostName addr) ":" (.getPort addr)))

(defn host-port [host:port]
  (let [vs (split host:port #":")
        host (first vs)
        port (Integer/valueOf ^String (second vs))]
    [host port]))


(def south-gate-handler
  (create-handler
   (on-message [ch msg addr]
               (let [addr (host:port addr)
                     conn&queue (get @south-connections addr)
                     queue (:queue conn&queue)
                     north-conn (peek @queue)]
                 (send north-conn msg)
                 (swap! queue pop)))
   (on-error [ch e]
             (.printStackTrace e)
             (let [conn&queue (get @south-connections
                                   (host:port (remote-addr ch)))
                   queue (:queue conn&queue)]
               (swap! queue pop)
               (close ch)))))

(def south-connection-factory
  (tcp-client-factory south-gate-handler
                      :encoder (redis-request-frame)
                      :decoder (redis-response-frame)))

(defn create-south-connection [host port]
  (tcp-client south-connection-factory host port 
              :lazy-connect true))

(defn add-south-redis [addr-str]
  (let [[host port] (host-port addr-str)]
    (swap! south-connections assoc
           addr-str
           {:conn (create-south-connection host port)
            :queue (atom (PersistentQueue/EMPTY))})))

(defn find-south-conn [msg]
  (let [{cmd :cmd key :key} msg
        node ((:grouping @conf) cmd key)
        instances (get (:groups @conf) node)]
    (map #(get @south-connections %) instances)))

(def north-gate-handler
  (create-handler
   (on-message [ch msg addr]
               (let [{conn :conn queue :queue}
                     (first (find-south-conn msg))]
                 (swap! queue conj ch)
                 (send conn (:packet msg))))
   (on-error [ch e]
             (.printStackTrace e)
             (close ch))))

(defn start-server [port]
  (tcp-server port north-gate-handler
              :decoder (redis-request-frame)
              :encoder (redis-response-frame)
              :tcp-options tcp-options))

