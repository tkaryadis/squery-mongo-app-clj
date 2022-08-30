(defproject org.squery/squery-mongo-app-clj "0.2.0-SNAPSHOT"
  :description "cmql-j app"
  :url "https://github.com/tkaryadis/cmql-app-clj"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/data.json "2.4.0"]
                 [org.squery/squery-mongo-core "0.2.0-SNAPSHOT"]
                 [org.squery/squery-mongo-j "0.2.0-SNAPSHOT"]]

  :source-paths ["src/clojure"]
  :java-source-paths ["src/java"]

  :aot [squery-mongo-app-clj.interop.quickstart-api]
  )
