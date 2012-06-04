(ns radar.core
  (:use [radar config server])
  (:gen-class))

(defn load-config [config-file]
  (binding [*ns* (create-ns (gensym "radar.config.user"))]
    (refer-clojure)
    (use '[radar.config])
    (load-file config-file)))

(defn all-servers [conf]
  (into #{} (apply concat (vals (:groups conf)))))

(defn -main [config-file & args]
  (let [config (load-config config-file)]
    (reset! conf config)
    (doseq [s (all-servers config)]
      (add-south-redis s)))
  (start-server 9099)
  (println "radar service ready."))
