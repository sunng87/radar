(ns radar.core
  (:use [radar server]))

(defn -main [& args]
  (start-server 9099))
