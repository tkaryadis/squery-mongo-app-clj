(ns cmql-app-clj.examples.forum36indexSelectivity
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
  (:import (com.mongodb.client MongoClients MongoCollection MongoDatabase MongoClient AggregateIterable)
           (com.mongodb MongoClientSettings)
           (java.sql Date)))


(update-defaults :client-settings (-> (MongoClientSettings/builder)
                                      (.codecRegistry clj-registry) ;;Remove this if you want to decode in Java Document
                                      (.build)))

(update-defaults :client (MongoClients/create ^MongoClientSettings (defaults :client-settings)))

(try (drop-collection :testdb.testcoll) (catch Exception e ""))

(defn add-docs [ndocs start]
  (loop [n start
         docs []]
    (if (= n (+ ndocs start))
      (insert :testdb.testcoll docs)
      (recur (inc n) (conj docs {:a (range 10000)  :b (even? n) :c n})))))

(dotimes [x 100] (add-docs 100 (* x 100)))


(create-index :testdb.testcoll (index [:b]))

(Thread/sleep 2000)

(time (c-take-all (q :testdb.testcoll
                     (= :b true)
                     (sort :c)
                     (limit 1)
                     )))