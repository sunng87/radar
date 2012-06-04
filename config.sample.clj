(radar-config
 (groups
  :s1 {:master "192.168.1.100:6379"
       :slaves ["192.168.1.101:6379"]}
  :s2 {:master "192.168.1.102:6379"})
 (grouping-fn [cmd key]
              (let [parts [:s1 :s2]]
                (nth  parts (mod (hash key) (count parts))))))
