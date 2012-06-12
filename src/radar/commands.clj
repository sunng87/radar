(ns radar.commands
  (:use [radar util])
  (:use [clojure.string :only [upper-case]]))

;; spec for commands
;; :rw [:r|:w] read or write
;; :supported [true|false] is supported by radar
;; :key [:one|:zero|:all] how many keys contains in it
;; :pass-proxy [true|false] response made by radar

(def redis-commands
  {"APPEND" {:rw :w},
   "AUTH" {:pass-proxy false :key :zero},
   "BGREWRITEAOF" {:rw :w :key :zero},
   "BGSAVE" {:rw :w :key :zero},
   "BITCOUNT" {},
   "BITOP" {:supported false}, ;;TODO
   "BLPOP" {:rw :w},
   "BRPOP" {:rw :w},
   "BRPOPLPUSH" {:supported false},
   "CONFIG GET" {:supported false},
   "CONFIG SET" {:supported false},
   "CONFIG RESETSTAT" {:rw :w},
   "DBSIZE" {:supported false},
   "DEBUG OBJECT" {},
   "DEBUG SEGFAULT" {:supported false},
   "DECR" {:rw :w},
   "DECRBY" {:rw :w},
   "DEL" {:rw :w :key :all},
   "DISCARD" {:supported false},
   "DUMP" {},
   "ECHO" {:pass-proxy true},
   "EVAL" {:supported false},
   "EXEC" {:supported false},
   "EXISTS" {},
   "EXPIRE" {:rw :w},
   "EXPIREAT" {:rw :w},
   "FLUSHALL" {:supported false},
   "FLUSHDB" {:supported false},
   "GET" {},
   "GETBIT" {},
   "GETRANGE" {},
   "GETSET" {:rw :w},
   "HDEL" {:rw :w},
   "HEXISTS" {},
   "HGET" {},
   "HGETALL" {},
   "HINCRBY" {:rw :w},
   "HINCRBYFLOAT" {:rw :w},
   "HKEYS" {},
   "HLEN" {},
   "HMGET" {},
   "HMSET" {:rw :w},
   "HSET" {:rw :w},
   "HSETNX" {:rw :w},
   "HVALS" {},
   "INCR" {:rw :w},
   "INCRBY" {:rw :w},
   "INCRBYFLOAT" {:rw :w},
   "INFO" {:pass-proxy true},
   "KEYS" {:supported false},
   "LASTSAVE" {:supported false},
   "LINDEX" {},
   "LINSERT" {:rw :w},
   "LLEN" {},
   "LPOP" {:rw :w},
   "LPUSH" {:rw :w},
   "LPUSHX" {:rw :w},
   "LRANGE" {},
   "LREM" {:rw :w},
   "LSET" {:rw :w},
   "LTRIM" {:rw :w},
   "MGET" {:supported false},
   "MIGRATE" {:supported false},
   "MONITOR" {:supported false},
   "MOVE" {:supported false},
   "MSET" {:supported false},
   "MSETNX" {:supported false},
   "MULTI" {:supported false},
   "OBJECT" {:supported false},
   "PERSIST" {:rw :w},
   "PEXPIRE" {:rw :w},
   "PEXPIREAT" {:rw :w},
   "PING" {:pass-proxy (fn [d] (to-buffer "+PONG\r\n"))},
   "PSETEX" {:rw :w},
   "PSUBSCRIBE" {:supported false},
   "PTTL" {},
   "PUBLISH" {:supported false},
   "PUNSUBSCRIBE" {:supported false},
   "QUIT" {:pass-proxy true},
   "RANDOMKEY" {:supported false},
   "RENAME" {:supported false},
   "RENAMENX" {:supported false},
   "RESTORE" {:supported false},
   "RPOP" {:rw :w},
   "RPOPLPUSH" {:supported false},
   "RPUSH" {:rw :w},
   "RPUSHX" {:rw :w},
   "SADD" {:rw :w},
   "SAVE" {:rw :w},
   "SCARD" {},
   "SCRIPT EXISTS" {:supported false},
   "SCRIPT FLUSH" {:supported false},
   "SCRIPT KILL" {:supported false},
   "SCRIPT LOAD" {:supported false},
   "SDIFF" {:supported false},
   "SDIFFSTORE" {:supported false},
   "SELECT" {:rw :w :key :zero},
   "SET" {:rw :w},
   "SETBIT" {:rw :w},
   "SETEX" {:rw :w},
   "SETNX" {:rw :w},
   "SETRANGE" {:rw :w},
   "SHUTDOWN" {:rw :w :key :zero},
   "SINTER" {:supported false},
   "SINTERSTORE" {:supported false},
   "SISMEMBER" {},
   "SLAVEOF" {:supported false},
   "SLOWLOG" {:supported false},
   "SMEMBERS" {},
   "SMOVE" {:supported false},
   "SORT" {:rw :w},
   "SPOP" {:rw :w},
   "SRANDMEMBER" {},
   "SREM" {:rw :w},
   "STRLEN" {},
   "SUBSCRIBE" {:supported false},
   "SUNION" {:supported false},
   "SUNIONSTORE" {:supported false},
   "SYNC" {:supported false},
   "TIME" {:pass-proxy true},
   "TTL" {},
   "TYPE" {},
   "UNSUBSCRIBE" {:supported false},
   "UNWATCH" {:supported false},
   "WATCH" {:supported false},
   "ZADD" {:rw :w},
   "ZCARD" {},
   "ZCOUNT" {},
   "ZINCRBY" {:rw :w},
   "ZINTERSTORE" {:supported false},
   "ZRANGE" {},
   "ZRANGEBYSCORE" {},
   "ZRANK" {},
   "ZREM" {:rw :w},
   "ZREMRANGEBYRANK" {:rw :w},
   "ZREMRANGEBYSCORE" {:rw :w},
   "ZREVRANGE" {},
   "ZREVRANGEBYSCORE" {},
   "ZREVRANK" {},
   "ZSCORE" {},
   "ZUNIONSTORE" {:supported false}})

(def supported-redis-commands
  (into {}
        (filter #(get (val %) :supported true) redis-commands)))

(defn get-key-spec [args cmd-spec]
  (let [key-spec (get cmd-spec :key :one)]
    (case key-spec
      :one (vector (to-string (second args)))
      :all (doall (map to-string (rest args)))
      :zero (vector))))

(defn get-rw-spec [cmd-spec]
  (get cmd-spec :rw :r))

(defn get-pass-proxy-spec [cmd-spec]
  (get cmd-spec :pass-proxy false))

(defn get-spec [args]
  (let [cmd-name (upper-case (to-string (first args)))]
    (if-let [cmd-spec (get supported-redis-commands cmd-name)]
      {:cmd cmd-name
       :key (get-key-spec args cmd-spec)
       :rw (get-rw-spec cmd-spec)
       :pass-proxy (get-pass-proxy-spec cmd-spec)})))

