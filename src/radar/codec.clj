(ns radar.codec
  (:use [link.codec :only [defcodec encoder decoder]])
  (:use [clojure.string :only [upper-case]])
  (:import [org.jboss.netty.buffer
            ChannelBuffer
            ChannelBuffers]))

(def key-aware-cmds
  #{"APPEND"
    "BITCOUNT"
    "BITOP"
    "BLPOP"
    "BRPOP"
    "DEBUG OBJECT"
    "DECR"
    "DECRBY"
    "DEL"
    "DUMP"
    "EVAL"
    "EXISTS"
    "EXPIRE"
    "EXPIREAT"
    "GET"
    "GETBIT"
    "GETRANGE"
    "GETSET"
    "HDEL"
    "HEXISTS"
    "HGET"
    "HGETALL"
    "HINCRBY"
    "HINCRBYFLOAT"
    "HKEYS"
    "HLEN"
    "HMGET"
    "HMSET"
    "HSET"
    "HSETNX"
    "HVALS"
    "INCR"
    "INCRBY"
    "INCRBYFLOAT"
    "LINDEX"
    "LINSERT"
    "LLEN"
    "LPOP"
    "LPUSH"
    "LPUSHX"
    "LRANGE"
    "LREM"
    "LSET"
    "LTRIM"
;;    "MGET"
    "MIGRATE"
    "MOVE"
;;    "MSET"
;;    "MSETNX"
    "PERSIST"
    "PEXPIRE"
    "PEXPIREAT"
    "PSETEX"
    "PTTL"
    "RENAME"
    "RENAMENX"
    "RESTORE"
    "RPOP"
    "RPUSH"
    "RPUSHX"
    "SADD"
    "SCARD"
    "SDIFF"
    "SDIFFSTORE"
    "SET"
    "SETBIT"
    "SETEX"
    "SETNX"
    "SETRANGE"
    "SINTER"
    "SINTERSTORE"
    "SISMEMBER"
    "SMEMBERS"
    "SORT"
    "SPOP"
    "SRANDMEMBER"
    "SREM"
    "STRLEN"
    "SUNION"
    "SUNIONSTORE"
    "TTL"
    "TYPE"
    "WATCH"
    "ZADD"
    "ZCARD"
    "ZCOUNT"
    "ZINCRBY"
    "ZINTERSTORE"
    "ZRANGE"
    "ZRANGEBYSCORE"
    "ZRANK"
    "ZREM"
    "ZREMRANGEBYRANK"
    "ZREMRANGEBYSCORE"
    "ZREVRANGE"
    "ZREVRANGEBYSCORE"
    "ZREVRANK"
    "ZSCORE"
    "ZUNIONSTORE"})

(defn- as-int [^String s]
  (Integer/valueOf s))

(defn- to-string [^bytes bytes]
  (if bytes (String. bytes)))
(defn- to-bytes [^String s]
  (if s (.getBytes s)))


(defmacro dbg [x]
  `(let [x# ~x]
     (println "dbg:" '~x "=" x#)
     x#))

(defn safe-readline [^ChannelBuffer buffer]
  (let [str-buf (StringBuilder.)]
    (.setLength str-buf 0)
    (loop []
      (if-let [d (if (.readable buffer) (.readByte buffer))]
        (when-not (or (< d 0) (= 10 d))
          (.append str-buf (char d))
          (recur))))
    (if (and
         (> (.length str-buf) 0)
         (= (.charAt str-buf (dec (.length str-buf))) \return))
      (subs (.toString str-buf) 0 (- (.length str-buf) 1)))))

(defn- read-bulk [^ChannelBuffer buffer]
  (if-let [first-line  (safe-readline buffer)]
    (let [arg-length (as-int (subs first-line 1))]
      (if (>= (.readableBytes buffer) (+ 2 arg-length))
        (let [data (byte-array arg-length)]
          (.readBytes buffer data)
          (.readByte buffer) ;;\r
          (.readByte buffer) ;;\n
          data)))))

(defn- wrap-bulk2 [^ChannelBuffer buffer bulk]
  (.writeBytes buffer
               ^bytes (to-bytes (str "$" (alength ^bytes bulk) "\r\n")))
  (.writeBytes buffer bulk)
  (.writeByte buffer 13) ;;\r
  (.writeByte buffer 10) ;;\n
  buffer)

(defn get-multibulk-size [args-bytes]
  (reduce #(+ %1 (count (str (alength %2))) 6 (alength %2))
          (+ 3 (count (str (count args-bytes))))
          args-bytes))

(defn wrap-multibulk [args-bytes]
  (if args-bytes
    (let [size (get-multibulk-size args-bytes)
          buffer (ChannelBuffers/buffer size)]
      (.writeBytes buffer
                   ^bytes (to-bytes (str "*" (count args-bytes) "\r\n")))
      (reduce #(wrap-bulk2 %1 %2) buffer args-bytes)
      buffer)))

(defn read-multibulk [^ChannelBuffer buffer]
  (if-let [first-line  (safe-readline buffer)]
    (let [args-count (as-int (subs first-line 1))]
      ;; return nil on nil
      (loop [result [] i 0]
        (if (= i args-count)
          result
          (if-let [r (read-bulk buffer)]
            (recur (conj result r) (inc i))))))))

(defcodec redis-request-frame
  (encoder [options ^ChannelBuffer data ^ChannelBuffer buffer]
           (.writeBytes buffer data)
           buffer)
  (decoder [options ^ChannelBuffer buffer]
           (let [args (read-multibulk buffer)]
             (if args
               (let [cmd (upper-case (to-string (first args)))
                     key (if (contains? key-aware-cmds cmd)
                           (to-string (second args)))]
                {:cmd cmd
                 :key key
                 :packet (wrap-multibulk args)})))))

(defn- wrap-line [prefix line]
  (if line
    (let [size (+ (alength ^bytes line) 2)
          buffer (ChannelBuffers/buffer size)]
      (.writeBytes buffer ^bytes line)
      (.writeByte buffer 13) ;;\r
      (.writeByte buffer 10) ;;\n
      buffer)))

(defn- wrap-bulk [data]
  (if data
    (let [size (alength ^bytes data)
          buffer (ChannelBuffers/buffer (+ size (count (str size)) 5))]
      (wrap-bulk2 buffer data))))

(defcodec redis-response-frame
  (encoder [options ^ChannelBuffer data ^ChannelBuffer buffer]
           (.writeBytes buffer data)
           buffer)
  (decoder [options ^ChannelBuffer buffer]
           (let [first-byte (.getByte buffer 0)]
             (case (char first-byte)
               \+ (wrap-line first-byte (to-bytes (safe-readline buffer)))
               \- (wrap-line first-byte (to-bytes (safe-readline buffer)))
               \: (wrap-line first-byte (to-bytes (safe-readline buffer)))
               \$ (wrap-bulk (read-bulk buffer))
               \* (wrap-multibulk (read-multibulk buffer))
               nil))))

