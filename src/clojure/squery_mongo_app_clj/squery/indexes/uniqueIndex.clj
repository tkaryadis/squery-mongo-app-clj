(ns squery-mongo-app-clj.squery.indexes.uniqueIndex
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
  (:require [clojure.core :as c]
            [clojure.data.json :as tojson])
  (:import (com.mongodb.client MongoClients MongoCollection MongoClient AggregateIterable)
           (com.mongodb MongoClientSettings)
           (org.bson.types ObjectId)
           (java.util Date Calendar)
           (com.mongodb.client.model Indexes)))


(update-defaults :client-settings (-> (MongoClientSettings/builder)
                                      ;(.codecRegistry clj-registry)
                                      (.build)))

(update-defaults :client (MongoClients/create ^MongoClientSettings (defaults :client-settings)))


(try (drop-collection :testdb.testcoll) (catch Exception e ""))


(insert :testdb.testcoll [{:name {:en "takis", "gr" "takhs"}},
                          {:name {:en "tahis", "gr" "tahhs"}}])

;;unique in the combination of names
(create-index :testdb.testcoll (index [:name.en :name.gr] {:unique true}))

;;this works
(prn (insert :testdb.testcoll [{:name {:en "takis"}}]))

;;this fails dupplicate
(prn (insert :testdb.testcoll [{:name {:en "takis" :de "takhs"}}]))

(c-print-all (q :testdb.testcoll))