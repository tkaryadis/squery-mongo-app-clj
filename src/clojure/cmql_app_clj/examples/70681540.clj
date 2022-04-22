(ns cmql-app-clj.examples.70681540
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

;(insert :testdb.testcoll [() (ordered-map)])

(def a (mapv (fn [a] [(str a) a]) (range 10000)))
(insert :testdb.testcoll [(into (ordered-map) a)])

(update- :testdb.testcoll
         (uq {:2 1000}))