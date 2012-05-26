(defproject radar "0.1.0-SNAPSHOT"
  :description "a redis proxy"
  :url "http://github.com/sunng87/radar"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [commons-pool "1.6"]
                 [link "0.3.3-SNAPSHOT"]]
  :warn-on-reflection true
  :main radar.core)
