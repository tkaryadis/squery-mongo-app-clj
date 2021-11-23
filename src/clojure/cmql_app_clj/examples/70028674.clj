(ns cmql-app-clj.examples.70028674
  (:refer-clojure :only [])
  (:use cmql-core.operators.operators
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
  (:import (com.mongodb.client MongoClients MongoCollection MongoDatabase MongoClient)
           (com.mongodb MongoClientSettings)
           (java.sql Date)))


(update-defaults :client-settings (-> (MongoClientSettings/builder)
                                      (.codecRegistry clj-registry) ;;Remove this if you want to decode in Java Document
                                      (.build)))

(update-defaults :client (MongoClients/create ^MongoClientSettings (defaults :client-settings)))

(try (drop-collection :testdb.testcoll) (catch Exception e ""))

;;db.jobs.updateMany({},
;; {$set: {'artifacts.$[elem]._id' : new ObjectId()}}, {arrayFilters: [ {'elem._id': {$exists: false}}]})

(insert :testdb.testcoll [{:artifacts [{:_id 0 :a 1} {:a 3} {:a 2}]}])

#_(update- :testdb.testcoll
  (uq {
       "$function" {"body" "function (ar) {ar.map(x => if(x._id) return x; else {x[\"_id\"]=new ObjectId(); return x;};}",
                    "args" [:artifacts],
                    "lang" "js"
                   }
       }))

;;    {x["_id"]=new ObjectId(); return x;}
(c-print-all (q :testdb.testcoll
                {:artifacts {
                             "$function" {"body" "function (ar) {return ar.map(x => { if(x.hasOwnProperty('_id')) return x; else {x[\"_id\"]=new ObjectId(); return x;}})}",
                                          "args" [:artifacts],
                                          "lang" "js"
                                          }
                             }}))