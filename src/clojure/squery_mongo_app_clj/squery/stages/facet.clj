(ns squery-mongo-app-clj.squery.stages.facet
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
  (:import (com.mongodb.client MongoClients MongoCollection MongoClient)
           (com.mongodb MongoClientSettings)))

(update-defaults :client-settings (-> (MongoClientSettings/builder)
                                      (.codecRegistry clj-registry) ;;Remove this if you want to decode in Java Document
                                      (.build)))

(update-defaults :client (MongoClients/create ^MongoClientSettings (defaults :client-settings)))

;;this facet use its used to implement the group-array squery operator

(try (drop-collection :testdb.testcoll) (catch Exception e ""))
(drop-collection :testdb.testcoll1)

(insert :testdb.testcoll [{:myarray [1 2 3]} {:myarray [4 5 6]}])

(insert :testdb.testcoll1 {})    ;;dummy collection of 1 document used in lookup


;;facet using a dummy collection with 1 empty document can allow add-from-pipeline
;;it will run in all documents,but all aggregation things run in all documents so its exactly the same
;;with unwind+group i get superfast reduce,i can use :REMOVE- to choose not to add a field or an array member
(c-print-all
(q :testdb.testcoll
   (lookup-p :testcoll1
             [:myarray- :myarray]
             (pipeline
               (facet {:aggr [{:myarray :myarray-}
                              (unwind :myarray)
                              (group nil
                                     {:sum (sum :myarray)}
                                     {:copy (conj-each (if- (> :myarray 3)
                                                            (+ :myarray 1)
                                                            :REMOVE-))})]}))
             :joined)
   {:sum (get-in :joined [0 "aggr" 0 "sum"])
    :copy (get-in :joined [0 "aggr" 0 "copy"])}))            ;;WHEN I HAVE KEYS I CAN DO THE GET FAST var1=get 0 var2=var1.aggr
