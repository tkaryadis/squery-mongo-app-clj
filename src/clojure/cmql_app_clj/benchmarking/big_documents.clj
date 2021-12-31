(ns cmql-app-clj.benchmarking.big-documents
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