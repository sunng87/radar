(ns radar.config)

;; a set of macro to config south nodes for radar
;;
;;


(defmacro grouping-fn [args & body]
  `{:grouping (fn ~args ~@body)})

(defn groups [& groups]
  {:groups (apply hash-map groups)})

(defmacro radar-config [& body]
  `(apply merge ~@body))

