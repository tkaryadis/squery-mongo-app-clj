(ns squery-mongo-app-clj.collections.nested.bench-nested-get
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


;;get-in seems very fast,like handmade code
;;more on website   collections/nested , perfomance

(update-defaults :client-settings (-> (MongoClientSettings/builder)
                                      (.codecRegistry clj-registry) ;;Remove this if you want to decode in Java Document
                                      (.build)))

(update-defaults :client (MongoClients/create ^MongoClientSettings (defaults :client-settings)))

#_(try (drop-collection :testdb.testcoll) (catch Exception e ""))

(defn add-docs [n]
  (let [docs (into [] (take n (repeat {
                                       "id" "some_id",
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
                                                             }]}]})))]
    (insert :testdb.testcoll docs)))

;;add one million documents like above
#_(dotimes [_ 1000] (add-docs 10000))


(time (.toCollection (q :testdb.testcoll
                        (out :testdb.testcoll1))))


(time (.toCollection (q :testdb.testcoll
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
                        (out :testdb.testcoll1))))

(time (.toCollection (q :testdb.testcoll
                        (= :_id "some_id")
                        [:!_id {:countValue (get-in :ROOT.
                                                    ["array1"
                                                     {:icond (= :v._id. "level_1_id")}
                                                     "array2"
                                                     1
                                                     "count"])}]
                        ;{:print true}
                        (out :testdb.testcoll1))))