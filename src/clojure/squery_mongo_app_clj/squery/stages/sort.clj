(ns squery-mongo-app-clj.squery.stages.sort
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

#_(try (drop-collection :testdb.testcoll) (catch Exception e ""))

(defn add-docs [n]
  (loop [n n
         docs []]
    (if (= n 0)
      (insert :testdb.testcoll docs)
      (recur (dec n) (conj docs {:a (rand-int 10000000)
                                 :d (into [] (range 10))
                                 })))))

;;add one million documents like above
#_(dotimes [_ 10000] (add-docs 1000))

;(def coll (.getCollection (.getDatabase (defaults :client) "testdb") "testcoll"))

(c-print-all (q :testdb.testcoll
                (sort :a)
                (limit 1)
                #_{:allowDiskUse true}))

#_(c-print-all (q :testdb.testcoll
                ;(limit 1)
                (group {"_id" nil}
                       {:docs (conj-each :ROOT.)})
                [:_id]
                {:allowDiskUse true}))




#_(println (clj->json (q :testdb.testcoll
                   ;(limit 1)
                   (group {"_id" nil}
                          {:docs (conj-each :ROOT.)})
                   [:_id]
                   {:allowDiskUse true}
                   (command))))


#_(time (prn (c-take-all (q :testdb.testcoll
                          ;(sort :natural)
                          (limit 4)))))

#_(time (prn (c-take-all (q :testdb.testcoll
                          (sort :a)
                          (limit 1)))))


