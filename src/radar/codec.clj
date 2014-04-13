(ns radar.codec
  (:use [radar util])
  (:use [link.codec :only [defcodec encoder decoder]])
  (:use [clojure.string :only [upper-case]])
  (:import [io.netty.buffer ByteBuf Unpooled]))


(defn safe-readline [^ByteBuf buffer]
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

(defn- read-bulk [^ByteBuf buffer]
  (if-let [first-line  (safe-readline buffer)]
    (let [arg-length (as-int (subs first-line 1))]
      (if (= arg-length -1)
        ;; maybe we need a better way for this situation, it only
        ;; happens when the server returns msg for key-not-found.
        :not-found
        (if (>= (.readableBytes buffer) (+ 2 arg-length))
          (let [data (byte-array arg-length)]
            (.readBytes buffer data)
            (.readByte buffer) ;;\r
            (.readByte buffer) ;;\n
            data))))))

(defn- wrap-bulk2 [^ByteBuf buffer bulk]
  (.writeBytes buffer
               ^bytes (to-bytes (str "$" (alength ^bytes bulk) "\r\n")))
  (.writeBytes buffer ^bytes bulk)
  (.writeByte buffer 13) ;;\r
  (.writeByte buffer 10) ;;\n
  buffer)

(defn get-multibulk-size [args-bytes]
  (reduce #(+ %1 (count (str (alength ^bytes %2))) 6 (alength ^bytes %2))
          (+ 3 (count (str (count args-bytes))))
          args-bytes))

(defn wrap-multibulk [args-bytes]
  (if args-bytes
    (case args-bytes
      :ping-inline (to-buffer "PING\r\n")
      (let [size (get-multibulk-size args-bytes)
            buffer (Unpooled/buffer size)]
        (.writeBytes buffer
                     ^bytes (to-bytes (str "*" (count args-bytes) "\r\n")))
        (reduce #(wrap-bulk2 %1 %2) buffer args-bytes)
        buffer))))

(defn read-multibulk [^ByteBuf buffer]
  (if-let [first-line  (safe-readline buffer)]
    (case (upper-case first-line)
      "PING" :ping-inline
      (let [args-count (as-int (subs first-line 1))]
       ;; return nil on nil
       (loop [result [] i 0]
         (if (= i args-count)
           result
           (if-let [r (read-bulk buffer)]
             (recur (conj result r) (inc i)))))))))

(defcodec redis-request-frame
  (encoder [options ^ByteBuf data ^ByteBuf buffer]
           (.writeBytes buffer data)
           buffer)
  (decoder [options ^ByteBuf buffer]
           (if-let [args (read-multibulk buffer)]
             {:data (case args
                      :ping-inline [(to-bytes "PING")]
                      args)
              :packet (wrap-multibulk args)})))

(defn- wrap-line [prefix line]
  (if line
    (let [size (+ (alength ^bytes line) 2)
          buffer (Unpooled/buffer size)]
      (.writeBytes buffer ^bytes line)
      (.writeByte buffer 13) ;;\r
      (.writeByte buffer 10) ;;\n
      buffer)))

(defn- wrap-bulk [data]
  (if data
    (if (= data :not-found)
      (Unpooled/wrappedBuffer ^bytes (to-bytes "$-1\r\n"))
      (let [size (alength ^bytes data)
            buffer (Unpooled/buffer (+ size (count (str size)) 5))]
        (wrap-bulk2 buffer data)))))

(defcodec redis-response-frame
  (encoder [options ^ByteBuf data ^ByteBuf buffer]
           (.writeBytes buffer data)
           buffer)
  (decoder [options ^ByteBuf buffer]
           (let [first-byte (.getByte buffer 0)]
             (case (char first-byte)
               \+ (wrap-line first-byte (to-bytes (safe-readline buffer)))
               \- (wrap-line first-byte (to-bytes (safe-readline buffer)))
               \: (wrap-line first-byte (to-bytes (safe-readline buffer)))
               \$ (wrap-bulk (read-bulk buffer))
               \* (wrap-multibulk (read-multibulk buffer))
               nil))))

(defn error-reply [msg]
  (to-buffer (str "-" msg)))
