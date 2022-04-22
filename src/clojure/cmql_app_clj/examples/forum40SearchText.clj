(ns cmql-app-clj.examples.forum40SearchText
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


(insert :testdb.testcoll [{"a" "77" "b" "77a"}
                           {"a" "78" "b" "77b"}])

(create-index :testdb.testcoll (index [{"a" "text"} {"b" "text"}]))

;(def coll (.getCollection (.getDatabase (defaults :client) "testdb") "testcoll"))
;(.createIndex coll (Indexes/text "a"))
;(.createIndex coll (Indexes/text "b"))

(c-print-all (q :testdb.testcoll
                { "$match"  { "$text"  { "$search" "77 77b" } } }
               ))


;;search only numbers


(try (drop-collection :testdb.testcoll) (catch Exception e ""))


(insert :testdb.testcoll [{"a" "1422112421" }
                          {"a" "1422112422" }])

(create-index :testdb.testcoll (index [{"a" "text"} ]))

(c-print-all (q :testdb.testcoll
                (text? "1422112421")))