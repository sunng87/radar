(ns radar.server
  (:refer-clojure :exclude [send])
  (:use [link core tcp])
  (:use [radar codec]))

(defn create-south-gate-handler [north-channel-ref]
  (create-handler
   (on-message [ch msg addr]
               (send @north-channel-ref msg))
   (on-error [ch e]
             (.printStackTrace e))))

(defn create-south-channel [host port]
  (let [upstream-channel-ref (atom nil)
        handler (create-south-gate-handler upstream-channel-ref)]
    {:south-channel (tcp-client host port handler
                                :encoder (redis-request-frame)
                                :decoder (redis-response-frame))
     :north-channel upstream-channel-ref}))


(def client (create-south-channel "127.0.0.1" 6379))
(defn init-session [key]
  ;; TODO get south channel from pool
  client)

;; init an object pool for south channel
;; TODO


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
              :threaded true
              :ordered true))

