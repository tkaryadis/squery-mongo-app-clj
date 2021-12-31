(ns cmql-app-clj.examples.forum29lookupFacetAlt
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


#_(try (drop-collection :testdb.testcoll) (catch Exception e ""))

(try (drop-collection :testdb.testcoll1) (catch Exception e ""))
(try (drop-collection :testdb.testcoll2) (catch Exception e ""))


(defn add-docs [ndocs start]
  (loop [n start
         docs []]
    (if (= n (+ ndocs start))
      (insert :testdb.testcoll docs)
      (recur (inc n) (conj docs {:a n})))))


#_(dotimes [x 100] (add-docs 10000 (* x 10000)))


;(def coll (.getCollection (.getDatabase (defaults :client) "testdb") "testcoll"))

(prn "started")

(insert :testdb.testcoll1 [{:a 1}{:a 1}{:a 1}])

(dotimes [_ 10000]
  (if (not= (get (first (c-take-all (q :testdb.testcoll1
                                       (sample 3)
                                       (group nil
                                              {:count (sum 1)})
                                       (= :count 3)
                                       [:!_id :count])))
                 :count)
            3)
    (prn "not3")))

#_(c-print-all (q :testdb.testcoll
                (lookup-p :testcoll
                          [(group {:_id nil}
                                  {:max (max :a)})]
                          :joined)
                {:max (get-in :joined [0 "max"])}
                (unset :joined)
                (sort :max)
                (limit 1)
                ))

#_(time (.toCollection (q :testdb.testcoll
                        {:max 999999}
                        (unset :joined)
                        (out :testdb.testcoll2))))

#_(time (.toCollection (q :testdb.testcoll
                        (lookup-p :testcoll
                                  [(group {:_id nil}
                                          {:max (max :a)})]
                                  :joined)
                        {:max (get-in :joined [0 "max"])}
                        (unset :joined)
                        (out :testdb.testcoll1))))