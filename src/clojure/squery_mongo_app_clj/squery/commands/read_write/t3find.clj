(ns squery-mongo-app-clj.squery.commands.read_write.t3find
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

(def docs [{ "_id"  1, :category  "food", "budget" 400, "spent" 450 }
           { "_id"  2, "category"  "drinks", "budget" 100, "spent" 150 }
           { "_id"  3, "category"  "clothes", "budget" 100, "spent" 50 }
           { "_id"  4, "category"  "misc", "budget" 500, "spent" 300 }
           { "_id"  5, "category"  "travel", "budget" 200, "spent" 650 }])

(insert :testdb.testcoll docs)

(println "----------After Insert------------")
(c-print-all (q :testdb.testcoll))

(println "----------After find------------")

(c-print-all (fq :testdb.testcoll
                 (> :spent 150)
                 [:!_id :spent {:a (+ :spent 20)}]
                 (sort :!spent)
                 (limit 1)
                 ;(command)
                 ))