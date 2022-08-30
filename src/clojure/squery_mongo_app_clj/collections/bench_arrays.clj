(ns squery-mongo-app-clj.collections.bench-arrays
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

;;Small benchmark,to compare, map/filter/reduce with conj/and squery reduce-array operator

(update-defaults :client-settings (-> (MongoClientSettings/builder) (.codecRegistry clj-registry) (.build)))

(update-defaults :client (MongoClients/create ^MongoClientSettings (defaults :client-settings)))

(try (drop-collection :testdb.testcoll) (catch Exception e ""))

;;to make reduce/group fast we need,to create squery collection to the database that we will use
;;squery collection is just a collection with 1 empty document
(drop-collection :testdb.squery)
(insert :testdb.squery {})

(defn add-docs [n-docs array-size]
  (let [docs (into [] (take n-docs (repeat {:myarray (into [] (range array-size))
                                            :myarray1 (into [] (range array-size))})))]
    (insert :testdb.testcoll docs)))

(time (dotimes [_ 500]
        (add-docs 2 100000)))

;;------------------------------MAP/FILTER (ARRAY->ARRAY)---------------------------------------------------------------

;; filter to keep some members

(time
  (.toCollection
    (q :testdb.testcoll
       [{:filtered (filter (fn [:m.] (> :m. 1)) :myarray)}]
       (out :testdb.tempout))))

#_(time
  (.toCollection (q :testdb.testcoll
                    [{:maped (map (fn [:m.] (+ :m. 1)) :myarray)}]
                    (out :testdb.tempout))))

#_(time
  (.toCollection (q :testdb.testcoll
                    [{:maped-filtered (filter (fn [:m.]
                                                :m.)
                                              (map (fn [:m.] (+ :m. 1)) :myarray))}]
                    (out :testdb.tempout))))

;;use only if < 500 members
#_(time
  (.toCollection (q :testdb.testcoll
                    [{:reduced1 (reduce (fn [:a. :n.]
                                          (conj :a. :n.))
                                        []
                                        :myarray)}]
                    (out :testdb.tempout))))

(time
  (.toCollection (q :testdb.testcoll
                    (unwind :myarray)
                    (group :_id
                           {:myarray (conj-each :myarray)})
                    (out :testdb.tempout)
                    {:allowDiskUse true})))

(time
  (.toCollection
    (q :testdb.testcoll
       (reduce-array :myarray
                     {:copy  (conj-each :a)})
       [:copy]
       (out :testdb.tempout))))