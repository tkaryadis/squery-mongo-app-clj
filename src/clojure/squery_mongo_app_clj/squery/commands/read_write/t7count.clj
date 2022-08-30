(ns squery-mongo-app-clj.squery.commands.read_write.t7count
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
        clojure.pprint)
  (:refer-clojure)
  (:require [clojure.core :as c])
  (:import (com.mongodb.client MongoClients MongoCollection MongoClient)
           (com.mongodb MongoClientSettings)))

(update-defaults :client-settings (-> (MongoClientSettings/builder)
                                      (.codecRegistry clj-registry) ;;Remove this if you want to decode in Java Document
                                      (.build)))

(update-defaults :client (MongoClients/create ^MongoClientSettings (defaults :client-settings)))

(try (drop-collection :testdb.testcoll) (catch Exception e ""))

(def docs [{ "_id" 1, "dept" "A", "item" { "sku" "111", "color" "red" }, "sizes" [ "S", "M" ] }
           { "_id" 2, "dept" "A", "item" { "sku" "111", "color" "blue" }, "sizes" [ "M", "L" ] }
           { "_id" 3, "dept" "B", "item" { "sku" "222", "color" "blue" }, "sizes" "S" }
           { "_id" 4, "dept" "A", "item" { "sku" "333", "color" "black" }, "sizes" [ "S" ] }])


(insert :testdb.testcoll docs)

(println "----------After Insert------------")
(c-print-all (q :testdb.testcoll))

(println "----------Count all------------")
(c-print-all (q-count :testdb.testcoll))

(println "----------Count with query------------")
(c-print-all (q-count :testdb.testcoll
                      (= :dept "A")                         ; 3 docs
                      (skip 1)                              ; 2 docs
                      (limit 1)                             ; 1 doc
                      ))
