(defproject radar "0.2.0-SNAPSHOT"
  :description "a redis proxy"
  :url "http://github.com/sunng87/radar"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [link "0.6.6"]]
  :global-vars {*warn-on-reflection* true}
  :profiles {:uberjar {:aot :all}}
  :main ^:skip-aot radar.core)
