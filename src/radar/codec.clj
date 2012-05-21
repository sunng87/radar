(ns radar.codec
  (:use [link.codec :only [defcodec encoder decoder]])
  (:use [clojure.string :only [upper-case]])
  (:import [org.jboss.netty.buffer
            ChannelBuffer
            ChannelBuffers
            ChannelBufferInputStream]))

(defn- as-int [s]
  (try
    (Integer/valueOf s)
    (catch NumberFormatException e nil)))

(defn- to-string [^bytes bytes]
  (String. bytes))
(defn- to-bytes [^String s]
  (.getBytes s))


(defmacro dbg [x]
  `(let [x# ~x]
     (println "dbg:" '~x "=" x#)
     x#))

(defn- read-bulk [^ChannelBufferInputStream ins]
  (if-let [arg-length (as-int (subs (.readLine ins) 1))]
    (if (>= (.available ins) arg-length)
      (let [data (byte-array arg-length)]
        (.readFully ins data)
        (.readByte ins) ;;\r
        (.readByte ins) ;;\n
        data))))

(defn- wrap-bulk2 [^ChannelBuffer buffer bulk]
  (.writeBytes buffer
               (to-bytes (str "$" (alength bulk) "\r\n")))
  (.writeBytes buffer bulk)
  (.writeBytes buffer (to-bytes "\r\n"))
  buffer)

(def key-aware-cmds
  #{"GET"
    "SET"})

(defn get-multibulk-size [args-bytes]
  (reduce #(+ %1 (count (str (alength %2))) 6 (alength %2))
          (+ 3 (count (str (count args-bytes))))
          args-bytes))

(defn wrap-multibulk [args-bytes]
  (let [size (get-multibulk-size args-bytes)
        buffer (ChannelBuffers/buffer size)]
    (.writeBytes buffer (to-bytes (str "*" (count args-bytes) "\r\n")))
    (reduce #(wrap-bulk2 %1 %2) buffer args-bytes)
    buffer))

(defn read-multibulk [^ChannelBufferInputStream ins]
  (let [first-line (.readLine ins)
        args-count (as-int (subs first-line 1))]
    (doall (map #(read-bulk %)
                (take args-count (repeat ins))))))

(defcodec redis-request-frame
  (encoder [options ^ChannelBuffer data ^ChannelBuffer buffer]
           (.writeBytes buffer data)
           buffer)
  (decoder [options ^ChannelBuffer buffer]
           (let [ins (ChannelBufferInputStream. buffer)
                 args (read-multibulk ins)]
             (if-not (some nil? args)
               (let [cmd (upper-case (to-string (first args)))
                     key (if (contains? key-aware-cmds cmd)
                           (to-string (second args)) )]
                {:cmd cmd
                 :key key
                 :packet (wrap-multibulk args)})))))

(defn- wrap-line [prefix line]
  (let [size (+ (alength line) 3)
        buffer (ChannelBuffers/buffer size)]
    (.writeByte buffer (int prefix))
    (.writeBytes buffer line)
    (.writeBytes buffer (to-bytes "\r\n"))
    buffer))

(defn- wrap-bulk [data]
  (let [size (alength data)
        buffer (ChannelBuffers/buffer (+ size (count (str size)) 5))]
    (wrap-bulk2 buffer data)))

(defcodec redis-response-frame
  (encoder [options ^ChannelBuffer data ^ChannelBuffer buffer]
           (.writeBytes buffer data)
           buffer)
  (decoder [options ^ChannelBuffer buffer]
           (let [ins (ChannelBufferInputStream. buffer)
                 first-byte (.getByte buffer 0)]
             (case (char first-byte)
               \+ (wrap-line first-byte (to-bytes (.readLine ins)))
               \- (wrap-line first-byte (to-bytes (.readLine ins)))
               \: (wrap-line first-byte (to-bytes (.readLine ins)))
               \$ (wrap-bulk (read-bulk ins))
               \* (wrap-multibulk (read-multibulk ins))))))

