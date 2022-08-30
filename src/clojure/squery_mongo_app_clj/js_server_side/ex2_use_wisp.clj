(ns squery-mongo-app-clj.js-server-side.ex2-use-wisp
  (:refer-clojure :only [])
  (:use squery-mongo-core.operators.operators
        squery-mongo-core.operators.qoperators
        squery-mongo-core.operators.uoperators
        squery-mongo-core.operators.stages
        squery-mongo-core.operators.options
        squery-mongo.driver.cursor
        squery-mongo.driver.document
        squery-mongo.driver.settings
        squery-mongo.driver.transactions
        squery-mongo.driver.utils
        squery-mongo.arguments
        squery-mongo.commands
        squery-mongo.macros
        flatland.ordered.map
        clojure.pprint
        squery-mongo-app-clj.server-side-js.ex1-js-wrappers)
  (:refer-clojure)
  (:require [clojure.core :as c])
  (:import (com.mongodb.client MongoClients MongoCollection MongoDatabase MongoClient)
           (com.mongodb MongoClientSettings)))


(update-defaults :client-settings (-> (MongoClientSettings/builder)
                                      (.codecRegistry clj-registry) ;;Remove this if you want to decode in Java Document
                                      (.build)))

(update-defaults :client (MongoClients/create ^MongoClientSettings (defaults :client-settings)))

(try (drop-collection :testdb.testcoll) (catch Exception e ""))

(def docs [{:_id 1 :mynumber 1 :mystring "a" :myarray [1] :myobject {:a 1}}
           {:_id 2 :mynumber 2 :mystring "b" :myarray [2] :myobject {:a 2}}
           {:_id 3 :mynumber 3 :mystring "c" :myarray [3] :myobject {:a 3}}
           {:_id 4 :mynumber 4 :mystring "d" :myarray [4] :myobject {:a 4}}])

(insert :testdb.testcoll docs)

;;if i dont have js/myloop.js,and i only jave js/lib/myloop.js or js/lib/myloop.wisp i need to compile first
;;(if i have js/lib/myloop.js its not compile, its only for combining dependencies)
(compile-functions :myloop)

;;myloop is written in wisp,and it has 1 dependency conj_js.js (see js/lib/myloop.wisp to see what code does)
;;squery compiles the wisp,adds the dependency,the wisp core,and generates the myloop.js standalone in js directory
(c-print-all (q :testdb.testcoll
                {:updatedarray (ejs :myloop
                                    [:myarray 20])}))

;;myloop has 1 dependency the conj_js function,even if the code is in wisp,it doesnt matter,wisp doesnt change
;;code that it doesnt recognize,we can use any custom js function in our wisp code