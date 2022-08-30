(ns squery-mongo-app-clj.memory.docSize16max
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
                                      (.codecRegistry clj-registry) ;;Remove this if you want to decode in Java Document
                                      (.build)))

(update-defaults :client (MongoClients/create ^MongoClientSettings (defaults :client-settings)))


(try (drop-collection :testdb.testcoll) (catch Exception e ""))

(defn add-docs [n]
  (loop [n n
         docs []]
    (if (= n 0)
      (insert :testdb.testcoll docs)
      (recur (dec n) (conj docs {:a 1
                                 :b [1 2 3 4 5 6 7 8 9 10 11 12 13]})))))

(do (try (drop-collection :testdb.testcoll) (catch Exception e ""))
    (dotimes [_ 100] (add-docs 1000)))

(c-print-all (q :testdb.testcoll
                (group {:_id nil}
                       {:docs (conj-each :ROOT.)})
                [:_id {:count (count :docs)}]               ;;if i add this it works even if the above is >16mb
                ))

;;; How big is 16MB document?


;;; 700.000 words of 10chars , 70000 lines =

(try (drop-collection :testdb.testcoll) (catch Exception e ""))

(defn add-docs [n]
  (loop [n n
         docs []]
    (if (= n 0)
      (insert :testdb.testcoll docs)
      (recur (dec n) (conj docs {:a (take 700000 (repeat "0123456789"))})))))

(do (try (drop-collection :testdb.testcoll) (catch Exception e ""))
    (dotimes [_ 1] (add-docs 1)))

(c-print-all (q :testdb.testcoll
                [{:sizeKB (div (bson-size :ROOT.) 1000)}]
                ))


(try (drop-collection :testdb.testcoll) (catch Exception e ""))

(defn add-docs [n]
  (loop [n n
         docs []]
    (if (= n 0)
      (insert :testdb.testcoll docs)
      (recur (dec n) (conj docs {:a (take 700000 (repeat "0123456789"))})))))

(do (try (drop-collection :testdb.testcoll) (catch Exception e ""))
    (dotimes [_ 1] (add-docs 1)))

(c-print-all (q :testdb.testcoll
                [{:sizeKB (div (bson-size :ROOT.) 1000)}]
                ))