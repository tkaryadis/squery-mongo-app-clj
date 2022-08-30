(ns squery-mongo-app-clj.squery.commands.admin.indexes
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

(def docs
  [{ "_id"  1, "category"  "food" "size" "a"}
   { "_id"  2, "category"  "food" "size" "a"}
   { "_id"  3, "category"  "clothes" "size" "a"}
   { "_id"  4, "category"  "misc" "size" "a"}
   { "_id"  5, "category"  "travel" "size" "a"}])

(insert :testdb.testcoll
        docs)

(create-index :testdb.testcoll (index [:category {:size "text"}]))

(c-print-all (get (coll-stats :testdb.testcoll) :indexSizes))


;inside auto-name    "fieldName_type_fieldName_type"

(c-print-all (drop-indexes :testdb.testcoll [[:category {:size "text"}]]))

(create-index :testdb.testcoll (index [:category {:size "text"}]))

(c-print-all (drop-indexes :testdb.testcoll ["category_1_size_text"]))

(create-index :testdb.testcoll (index [:category {:size "text"}]))

(c-print-all (drop-index :testdb.testcoll [:category {:size "text"}]))

(create-index :testdb.testcoll (index [:category {:size "text"}]))

(c-print-all (drop-index :testdb.testcoll "category_1_size_text"))