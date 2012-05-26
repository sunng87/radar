(ns radar.core
  (:use [radar server])
  (:gen-class))

(defn -main [& args]
  (add-south-redis "localhost" 6380)
  (start-server 9099)
  (println "radar service ready."))
