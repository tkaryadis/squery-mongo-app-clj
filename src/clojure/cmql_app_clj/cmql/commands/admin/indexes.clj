(ns cmql-app-clj.cmql.commands.admin.indexes
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