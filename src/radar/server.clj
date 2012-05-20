(ns radar.server
  (:refer-clojure :exclude [send])
  (:use [link core tcp])
  (:use [radar codec]))

(def north-gate-handler
  (create-handler
   (on-message [ch msg addr]
               ;;TODO
               (println msg))
   (on-error [ch e]
             (.printStackTrace e))))


(defn start-server [port]
  (tcp-server port north-gate-handler
              :decoder (redis-request-frame)))

