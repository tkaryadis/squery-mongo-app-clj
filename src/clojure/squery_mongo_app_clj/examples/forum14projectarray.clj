(ns squery-mongo-app-clj.examples.forum14projectarray
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

;;https://www.mongodb.com/community/forums/t/accessing-element-of-object-array-by-index-in-one-project-step/117416

(update-defaults :client-settings (-> (MongoClientSettings/builder)
                                      (.codecRegistry clj-registry) ;;Remove this if you want to decode in Java Document
                                      (.build)))

(update-defaults :client (MongoClients/create ^MongoClientSettings (defaults :client-settings)))

(try (drop-collection :testdb.testcoll) (catch Exception e ""))

(def doc {
          "foo" [
                  {
                   "bar" {
                           "baz" "qux"
                          }
                   }
                  ]
          })

;;question= for group.# = 1 ,get group.end,and return the subgroup where  group.end==subgroup.#

(insert :testdb.testcoll doc)

(println (clj->json (q :testdb.testcoll
                       [:!_id {:baz (let [:firstMember. (get :foo 0)]
                                      :firstMember.bar.baz.)}]
                       {:command true})))
