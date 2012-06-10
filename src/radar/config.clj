(ns radar.config
  (:use [radar util]))

;; a set of macro to config south nodes for radar
;;
;;


(defn listen-port [port]
  {:port port})

(defmacro grouping-fn [args & body]
  `{:grouping (fn ~args ~@body)})

(defn groups [& groups]
  (let [nodes (apply hash-map groups)]
    {:groups (into {} (for [[k v] nodes]
                        [k (apply list (:master v) (:slaves v))]))}))

(defmacro radar-config [& body]
  `(apply merge ~@body))

(defonce conf (atom nil))

