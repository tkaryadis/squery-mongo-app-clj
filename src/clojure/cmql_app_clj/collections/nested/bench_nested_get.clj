(ns cmql-app-clj.collections.nested.bench-nested-get
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