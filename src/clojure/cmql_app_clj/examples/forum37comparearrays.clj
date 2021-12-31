(ns cmql-app-clj.examples.forum37comparearrays
  (:refer-clojure :only [])
  (:use cmql-core.operators.operators
        cmql-core.operators.qoperators
        cmql-core.operators.uoperators
        cmql-core.operators.stages
        cmql-core.operators.options
        cmql-j.driver.cursor
        cmql-j.driver.document
        cmql-j.driver.settings
        cmql-j.driver.transactions
        cmql-j.driver.utils
        cmql-j.arguments
        cmql-j.commands
        cmql-j.macros
        flatland.ordered.map
        clojure.pprint)
  (:refer-clojure)
  (:require [clojure.core :as c])
  (:import (com.mongodb.client MongoClients MongoCollection MongoDatabase MongoClient AggregateIterable)
           (com.mongodb MongoClientSettings)
           (java.sql Date)))


(update-defaults :client-settings (-> (MongoClientSettings/builder)
                                      (.codecRegistry clj-registry) ;;Remove this if you want to decode in Java Document
                                      (.build)))

(update-defaults :client (MongoClients/create ^MongoClientSettings (defaults :client-settings)))

(try (drop-collection :testdb.testcoll) (catch Exception e ""))

(insert :testdb.testcoll {
                          "_id"  12,
                          "arrayField" [ 0 5]         ;;[ 128, 190, 1 ]
                          })

(def coll (.getCollection (.getDatabase (defaults :client) "testdb") "testcoll"))

(c-print-all (q :testdb.testcoll))

(.updateOne ^MongoCollection
         coll
         (d {})
         (d {"$max" { "arrayField" -1} }))



(c-print-all (q :testdb.testcoll))