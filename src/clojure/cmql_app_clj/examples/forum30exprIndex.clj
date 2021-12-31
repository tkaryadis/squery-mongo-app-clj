(ns cmql-app-clj.examples.forum30exprIndex
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
  (:require [clojure.core :as c]
            [clojure.data.json :as tojson])
  (:import (com.mongodb.client MongoClients MongoCollection MongoClient AggregateIterable)
           (com.mongodb MongoClientSettings)
           (org.bson.types ObjectId)
           (java.util Date Calendar)
           (com.mongodb.client.model Indexes)))


;;https://stackoverflow.com/questions/68871092/check-if-a-documents-value-is-in-an-array-mongodb

(update-defaults :client-settings (-> (MongoClientSettings/builder)
                                      (.codecRegistry clj-registry)
                                      (.build)))

(update-defaults :client (MongoClients/create ^MongoClientSettings (defaults :client-settings)))


;(try (drop-collection :testdb.testcoll) (catch Exception e ""))

(defn add-docs [ndocs start]
  (loop [n start
         docs []]
    (if (= n (+ ndocs start))
      (insert :testdb.testcoll docs)
      (recur (inc n) (conj docs {:a n :b n})))))

;(insert :testdb.testcoll1 [{}])

;(dotimes [x 100] (add-docs 10000 (* x 10000)))

(create-index :testdb.testcoll (index [:a]))

(time (c-take-all (q :testdb.testcoll
                      (contains? [66688 66688] :a))))

(time (c-take-all (q :testdb.testcoll
                      (or (= 66688 :a) (= 66689 :a)))))

(time (c-take-all (q :testdb.testcoll
                     {"$match" {"a" {"$in" [66688 66688]}}})))