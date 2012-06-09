(ns radar.util)

(defn as-int [^String s]
  (Integer/valueOf s))

(defn to-string [^bytes bytes]
  (if bytes (String. bytes)))
(defn to-bytes [^String s]
  (if s (.getBytes s)))


(defmacro dbg [x]
  `(let [x# ~x]
     (println "dbg:" '~x "=" x#)
     x#))
