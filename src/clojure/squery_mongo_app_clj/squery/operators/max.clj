(ns squery-mongo-app-clj.squery.operators.max
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

(update-defaults :client-settings (-> (MongoClientSettings/builder)
                                      (.codecRegistry clj-registry)
                                      (.build)))

(update-defaults :client (MongoClients/create ^MongoClientSettings (defaults :client-settings)))


(try (drop-collection :testdb.testcoll) (catch Exception e ""))

(defn add-docs [n]
  (loop [n n
         docs []]
    (if (= n 0)
      (insert :testdb.testcoll (into [] (shuffle docs)))
      (recur (dec n) (conj docs {:m (ordered-map "a"  (rand-int 10000)
                                                 "b"  (rand-int 10000)
                                                 "c"  (rand-int 10000)
                                                 "d"  (rand-int 10000)
                                                 "e"  (rand-int 10000)
                                                 "f"  (rand-int 10000)
                                                 "g"  (rand-int 10000))
                                 :n (if (even? n) 0 1)})))))

;;add one million documents like above
(dotimes [_ 1000] (add-docs 1000))

(c-print-all (q :testdb.testcoll
                (group :n
                       {:max (max (ordered-map "a"  :m.a
                                               "b"  :m.b
                                               "c"  :m.c
                                               "d"  :m.d
                                               "e"  :m.e
                                               "f"  :m.f
                                               "g"  :m.g))})))

(c-print-all (q :testdb.testcoll
                (group :n
                       {:max (max :m)})))

(c-print-all (q :testdb.testcoll
                (group :n
                       {:max (max :m)})))

(c-print-all (q :testdb.testcoll
                (group :n
                       {:max (max :m)})))

(c-print-all (q :testdb.testcoll
                (group :n
                       {:max (max :m)})))

(c-print-all (q :testdb.testcoll
                (group :n
                       {:max (max :m)})))

(c-print-all (q :testdb.testcoll
                (group :n
                       {:max (max :m)})))