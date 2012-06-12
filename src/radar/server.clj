(ns radar.server
  (:refer-clojure :exclude [send])
  (:use [clojure.string :only [split]])  
  (:use [link core tcp pool])
  (:use [radar codec commands])
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
               ;; FIXME we don't always have to pop here
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

(defn find-south-conn [cmd-info]
  (let [{cmd :cmd keys :key rw :rw} cmd-info
        ;; currently radar doesn't support command with multiple keys
        group ((:grouping @conf) cmd (first keys))
        ;; do not use mapcat here
        instances (get (:groups @conf) group)]
    (case rw
      :w (map #(get @south-connections %) instances)
      :r [(get @south-connections (rand-nth instances))])))

(def north-gate-handler
  (create-handler
   (on-message [ch msg addr]
               (let [{packet :packet data :data} msg]
                 (when-let [cmd-info (get-spec data)]
                   (if-not (:pass-proxy cmd-info)
                     (doseq [{conn :conn queue :queue}
                             (find-south-conn cmd-info)]
                       (swap! queue conj ch)
                       (send conn packet))
                     (send ch ((:pass-proxy cmd-info) data))))))
   (on-error [ch e]
             (.printStackTrace e)
             (close ch))))

(defn start-server [port]
  (tcp-server port north-gate-handler
              :decoder (redis-request-frame)
              :encoder (redis-response-frame)
              :tcp-options tcp-options))

