(ns squery-mongo-app-clj.examples.forum2-nested-get-update
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
  (:import (com.mongodb.client MongoClients MongoCollection MongoDatabase MongoClient)
           (com.mongodb MongoClientSettings)))

(update-defaults :client-settings (-> (MongoClientSettings/builder)
                                      (.codecRegistry clj-registry) ;;Remove this if you want to decode in Java Document
                                      (.build)))

(update-defaults :client (MongoClients/create ^MongoClientSettings (defaults :client-settings)))

(try (drop-collection :testdb.testcoll) (catch Exception e ""))

;;How do I retrieve the count of the element with _id:
;;level_2_id_1? All I know is the _id of the root document,
;;the _id of the element in array1, and the index of array2 I need. I tried the following, with index = 0:

(insert :testdb.testcoll
        {
         "_id" "some_id",
         "array1" [
                   {
                    "_id" "level_1_id"
                    "array2" [
                              {
                               "_id" "level_2_id_0"
                               "count" 0
                               }
                              {
                               "_id" "level_2_id_1"
                               "count" 1
                               }]}]})

;;-------------------------------------using squery get-in/assoc-in-------------------------------------------------------

(c-print-all (q :testdb.testcoll
                (= :_id "some_id")
                [:!_id {:countValue (get-in :ROOT.
                                            ["array1"
                                             {:icond (= :v._id. "level_1_id")}
                                             "array2"
                                             1
                                             "count"])}]
                ;{:print true}
                ))

(c-print-all (update- :testdb.testcoll
               (uq (= :_id "some_id")
                   (replace-root (assoc-in  :ROOT.
                                            ["array1"
                                             {:icond (= :v._id. "level_1_id")}
                                             "array2"
                                             1
                                             "count"]
                                            100)))
               {:print true}
               ))


;;-----------------------------Handmade solution------------------------------------------------------------------------

#_(c-print-all (q :testdb.testcoll
                (= :_id "some_id")
                [:!_id {:countValue (let [:array1. :ROOT.array1.
                                          :array1-member. (get (filter (fn [:m.]
                                                                         (= :m._id. "level_1_id"))
                                                                       :array1.)
                                                               0)
                                          :array2. :array1-member.array2.
                                          :array2-member. (get :array2. 1)
                                          ]
                                      :array2-member.count.)}]
                {:print true}
                ))

#_(c-print-all (update- :testdb.testcoll
               (uq (= :_id "some_id")
                   (replace-root (merge :ROOT.
                                        {:array1 (map (fn [:m.]
                                                        (if- (= :m._id. "level_1_id")
                                                             (merge :m.
                                                                    {:array2 (map (fn [:m.]
                                                                                    (if- (= :m._id. "level_2_id_1")
                                                                                         (merge :m.
                                                                                                {:count 100})
                                                                                         :m.))
                                                                                  :m.array2.)})
                                                             :m.))
                                                      :array1)})))
               ;{:print true}
               ))