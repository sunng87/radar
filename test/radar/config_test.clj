(ns radar.config-test
  (:use clojure.test)
  (:use radar.config))

(deftest config-demo-test
  (let [config (radar-config
                (groups
                 :s1 {:master "192.168.1.101:6379"
                      :slaves ["192.168.1.103:6379"]}
                 :s2 {:master "192.168.1.102:6379"
                      :slaves ["192.168.1.104:6379"]})
                (grouping-fn [cmd key]
                             (let [parts [:s1 :s2]]
                               (nth  parts (mod (hash key) (count parts))))))]
    (is (= 2 (count (:groups config))))
    (is (= [:s1 :s2] (keys (:groups config))))
    (is (= (list "192.168.1.101:6379" "192.168.1.103:6379")
           (:s1 (:groups config))))
    (is (fn? (:grouping config)))
    (is (contains? #{:s1 :s2} ((:grouping config) "get" "900913")))))

