(ns squery-mongo-app-clj.interop.quickstart-api
  (:require [squery-mongo-app-clj.quickstart_clojure.quick-commands-methods])
  (:gen-class
    :name squery_mongo_app_clj.interop.Quickstart_api
    :methods [#^{:static true} [helloClojureFromJava [String] String]
              #^{:static true} [runMethods [] void]
              #^{:static true} [runCommands [] void]]))

;;This is a example of a normal Java class created from Clojure,that allows us to call the code
;;from Java like calling normal static methods

;;We will use this from to call from java,in the javaappmaven app

(defn ^String -helloClojureFromJava [s]
  (println "Hello " s "!I send you back a Java string.")
  "Clojure is Magic!")

(defn -runMethods []
  (squery-mongo-app-clj.quickstart_clojure.quick-commands-methods/run-methods))

(defn -runCommands []
  (squery-mongo-app-clj.quickstart_clojure.quick-commands-methods/run-commands))




