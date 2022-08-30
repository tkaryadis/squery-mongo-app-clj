(ns squery-mongo-app-clj.examples.forum31lookupOptimize
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
  (:require [clojure.core :as c]
            [clojure.data.json :as tojson])
  (:import (com.mongodb.client MongoClients MongoCollection MongoClient AggregateIterable)
           (com.mongodb MongoClientSettings)
           (org.bson.types ObjectId)
           (java.util Date Calendar)
           (com.mongodb.client.model Indexes)))


(update-defaults :client-settings (-> (MongoClientSettings/builder)
                                      (.codecRegistry clj-registry)
                                      (.build)))

(update-defaults :client (MongoClients/create ^MongoClientSettings (defaults :client-settings)))

;;https://stackoverflow.com/questions/69525075/how-to-do-a-conditional-lookup-in-mongodb/69525675#69525675

;(try (drop-collection :testdb.testcoll1) (catch Exception e ""))
;(try (drop-collection :testdb.testcoll2) (catch Exception e ""))

(defn add-docs [ndocs start]
  (loop [n start
         docs []]
    (if (= n (+ ndocs start))
      (do (insert :testdb.testcoll1 docs)
          (insert :testdb.testcoll2 docs))
      (recur (inc n) (conj docs {:a n :b n})))))


;(dotimes [x 100] (add-docs 10000 (* x 10000)))

(time (.toCollection (q :testdb.testcoll1
                        (lookup-p :testdb.testcoll2
                                  [:a. :a]
                                  [(= :a :a.)
                                   (= false true)]
                                  :joined)
                        (out :testdb.testcoll3))))

(time (.toCollection (q :testdb.testcoll1
                        (lookup-p :testdb.testcoll2
                                  [:a. :a]
                                  [(= :a. -1)]
                                  :joined)
                        (out :testdb.testcoll3))))



#_(time (.toCollection (q :testdb.testcoll1
                        (lookup-p :testdb.testcoll2
                                  [:a. :a]
                                  [(= :a. :a)]
                                  :joined)
                        (out :testdb.testcoll3))))

(time (.toCollection (q :testdb.testcoll1
                        {:joined (let [:a. :a]
                                   (if- (= :a. -1)
                                     [1]
                                     []))}
                        (out :testdb.testcoll3))))

(time (.toCollection (q :testdb.testcoll1
                        (lookup-p :testdb.testcoll2
                                  [(= 1 0)]
                                  :joined)
                        (out :testdb.testcoll3))))