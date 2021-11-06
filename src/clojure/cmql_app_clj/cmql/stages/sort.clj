(ns cmql-app-clj.cmql.stages.sort
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
        flatland.ordered.map
        clojure.pprint)
  (:refer-clojure)
  (:require [clojure.core :as c])
  (:import (com.mongodb.client MongoClients MongoCollection MongoClient)
           (com.mongodb MongoClientSettings)
           (org.bson.types ObjectId)
           (com.mongodb.client.model UpdateOptions Filters)
           (java.util Collections Arrays)))

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


