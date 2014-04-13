(ns radar.util
  (:import [io.netty.buffer Unpooled]))

(defn as-int [^String s]
  (Integer/valueOf s))

(defn to-string [^bytes bytes]
  (if bytes (String. bytes)))
(defn to-bytes [^String s]
  (if s (.getBytes s)))
(defn to-buffer [^String s]
  (Unpooled/wrappedBuffer ^bytes (to-bytes s)))


(defmacro dbg [x]
  `(let [x# ~x]
     (println "dbg:" '~x "=" x#)
     x#))
