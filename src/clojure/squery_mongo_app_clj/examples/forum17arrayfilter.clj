(ns squery-mongo-app-clj.examples.forum17arrayfilter
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
  (:import (com.mongodb.client MongoClients)
           (com.mongodb MongoClientSettings)))

(update-defaults :client-settings (-> (MongoClientSettings/builder)
                                      (.codecRegistry clj-registry) ;;Remove this if you want to decode in Java Document
                                      (.build)))

(update-defaults :client (MongoClients/create ^MongoClientSettings (defaults :client-settings)))

(try (drop-collection :testdb.testcoll) (catch Exception e ""))

;;https://stackoverflow.com/questions/68666620/map-through-array-and-return-all-matches-mongodb/68668214#68668214

(def data {:_id    1
           :orders [
                    {
                     "foodId"  1
                     "placeId" 2
                     "qty"     3
                     "price"   4
                     }
                    {
                     "foodId"  5
                     "placeId" 6
                     "qty"     7
                     "price"   8
                     }]})

(insert :testdb.testcoll data)

(c-print-all (q :testdb.testcoll
                {:orders (filter (fn [:order.]
                                   (= :order.placeId. 2))
                                 :orders)}))