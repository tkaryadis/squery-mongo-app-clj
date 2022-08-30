(ns squery-mongo-app-clj.squery.commands.read_write.t2delete
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

;;the doc to insert
(def doc  {
           "_id"  1
           "name"  "MongoDB"
           "type"  "database"
           "versions" [ "v3.2" "v3.0" "v2.6" ]
           "info" { "x"  203 "y"  102 }
           })

(def docs [{
            "_id"  2
            "name"  "MongoDB"
            "type"  "database"
            "versions" [ "v3.2" "v3.0" "v2.6" ]
            "info" { "x"  203 "y"  102 }
            }
           {
            "_id"  3
            "name"  "MongoDB"
            "type"  "database"
            "versions" [ "v3.2" "v3.0" "v2.6" ]
            "info" { "x"  203 "y"  102 }
            }])

;;Clojure maps are converted to valid Json Documents
;;for example symbols/keywords to strings   vectors to arrays etc


;;print all document of the collection,including the one we inserted
(insert :testdb.testcoll
        doc)

(insert :testdb.testcoll docs)

(println "----------After Insert------------")
(c-print-all (q :testdb.testcoll))

;;--------------------------------Delete 1 query---------------------------------------------------

(delete :testdb.testcoll
        (dq (= :_id 1)
            (= :name "MongoDB")))

(println "----------After Delete------------")
(c-print-all (q :testdb.testcoll))

;;--------------------------------Delete vec of queries---------------------------------------------

(def dqs [(dq (= :_id 2)
              (= :name "MongoDB"))
          (dq (= :_id 3))])

(delete :testdb.testcoll
        dqs
        {:ordered true})

(println "----------After Delete------------")
(c-print-all (q :testdb.testcoll))