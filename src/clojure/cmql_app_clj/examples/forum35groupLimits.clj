(ns cmql-app-clj.examples.forum35groupLimits
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
      (recur (inc n) (conj docs {:a (rand-int 400)  :b (range 130)})))))

(dotimes [x 1] (add-docs 10000 (* x 10000)))

(c-print-all (.allowDiskUse ^AggregateIterable
                            (q :testdb.testcoll
                               (group {:_id :a}
                                      {:docs (conj-each :ROOT.)})
                               {:docs (count :docs)})
                            false))

(try (drop-collection :testdb.testcoll) (catch Exception e ""))

(defn add-docs [ndocs start]
  (loop [n start
         docs []]
    (if (= n (+ ndocs start))
      (insert :testdb.testcoll docs)
      (recur (inc n) (conj docs {:a n  :b (range 20000)})))))

(dotimes [x 1] (add-docs 100 (* x 100)))

(c-print-all (.allowDiskUse ^AggregateIterable
                            (q :testdb.testcoll
                               (group {:_id :a}
                                      {:docs (conj-each :ROOT.)})
                               (count-s))
                            true))





#_(c-print-all (.allowDiskUse ^AggregateIterable
                            (q :testdb.testcoll
                               (group {:_id nil}
                                      {:docs (conj-each :ROOT.)})
                               {:docs 1}
                               )
                            true))



