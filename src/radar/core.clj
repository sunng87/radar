(ns radar.core
  (:use [radar server]))

(defn -main [& args]
  (add-south-redis "localhost" 6379)
  (start-server 9099)
  (println "radar service ready."))
