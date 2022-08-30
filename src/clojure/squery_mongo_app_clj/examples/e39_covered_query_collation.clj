(ns squery-mongo-app-clj.examples.e39-covered-query-collation
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
        clojure.pprint)
  (:refer-clojure)
  (:require [clojure.core :as c])
  (:import (com.mongodb.client MongoClients MongoCollection MongoDatabase MongoClient AggregateIterable FindIterable)
           (com.mongodb MongoClientSettings)
           (java.sql Date)
           (com.mongodb.client.model Collation)))


(update-defaults :client-settings (-> (MongoClientSettings/builder)
                                      (.codecRegistry clj-registry) ;;Remove this if you want to decode in Java Document
                                      (.build)))

(update-defaults :client (MongoClients/create ^MongoClientSettings (defaults :client-settings)))

(try (drop-collection :testdb.testcoll) (catch Exception e ""))

(insert :testdb.testcoll [{ "_id"  1, "name"  "topolino" }
                          { "_id"  2, "name"  "tòpolino" }])

(create-index :testdb.testcoll (index [:name] {:collation {:locale "it"}}))

(def coll (.getCollection (.getDatabase (defaults :client) "testdb") "testcoll"))

;;collation cant be covered because 1-to-many matches, needs to read doc to get the real value
(pprint (.explain  ^FindIterable
                   (.collation ^FindIterable (.sort (.projection ^FindIterable
                                                   (.find coll
                                                          (d  {:name "tòpolino"}))
                                                   (d {:_id 0 :name 1}))
                                      (d {:name 1}))
                               (.build (.locale (Collation/builder)
                                                "it")))))