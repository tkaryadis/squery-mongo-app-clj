(ns squery-mongo-app-clj.benchmarking.big-documents
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
        clojure.pprint)
  (:refer-clojure)
  (:require [clojure.core :as c])
  (:import (com.mongodb.client MongoClients MongoCollection MongoClient)
           (com.mongodb MongoClientSettings)))


(update-defaults :client-settings (-> (MongoClientSettings/builder)
                                      (.codecRegistry clj-registry) ;;Remove this if you want to decode in Java Document
                                      (.build)))

(update-defaults :client (MongoClients/create ^MongoClientSettings (defaults :client-settings)))

#_(try (drop-collection :testdb.testcoll) (catch Exception e ""))

(defn add-docs [n]
  (let [docs (into [] (take n (repeat {:a       n
                                       :myarray (into [] (range 100000))})))]
    (insert :testdb.testcoll docs)))

;;add one million documents like above
;(dotimes [_ 100] (add-docs 10))

#_(try (drop-collection :testdb.testcoll1) (catch Exception e ""))

(defn add-docs [n]
  (let [docs (into [] (take n (repeat {:a       n
                                       :myarray [n]})))]
    (insert :testdb.testcoll1 docs)))

;;add one million documents like above
;(dotimes [_ 100] (add-docs 10))

;(create-index :testdb.testcoll (index [:a]))
;(create-index :testdb.testcoll1 (index [:a]))

;(drop-index :testdb.testcoll [:a])
;(drop-index :testdb.testcoll1 [:a])


#_(time (prn (count (c-take-all (q :testdb.testcoll
                                 (and (> :a 40) (< :a 60))
                                 [:a])))))

#_(time (prn (count (c-take-all (q :testdb.testcoll1
                                 (and (> :a 40) (< :a 60))
                                 [:a])))))

#_(time (prn (count (c-take-all (q :testdb.testcoll
                                 (and (> :a 40) (< :a 60))
                                 [:a])))))


#_(time (update- :testdb.testcoll (uq {:a (inc :a)})))

(def coll1 (.getCollection (.getDatabase (defaults :client) "testdb") "testcoll1"))
(def coll (.getCollection (.getDatabase (defaults :client) "testdb") "testcoll"))

(time (.updateMany coll (d {}) (d {"$set" {"a" 2}})))
(time (.updateMany coll1 (d {}) (d {"$set" {"a" 2}})))

(time (.updateMany coll (d {}) (d {"$set" {"a" 2}})))