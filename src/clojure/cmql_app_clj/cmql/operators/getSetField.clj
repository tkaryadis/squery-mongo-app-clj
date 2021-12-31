(ns cmql-app-clj.cmql.operators.getSetField
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

(insert :testdb.testcoll {:doc {:a 1 :b 2} :s "aa"})

;;{
;  $setField: {
;    field: <String>,
;    input: <Object>,
;    value: <Expression>
;  }
;}

(c-print-all (q :testdb.testcoll
                {:adoc {
                        "$setField" {
                                     "field"  "c"
                                     "input"  :doc
                                     "value"  20}
                        }}))