(ns cmql-app-clj.cmql.stages.groupAndMemory
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
  (:require [clojure.core :as c])
  (:import (com.mongodb.client MongoClients MongoCollection MongoClient)
           (com.mongodb MongoClientSettings)))


(update-defaults :client-settings (-> (MongoClientSettings/builder)
                                      (.codecRegistry clj-registry) ;;Remove this if you want to decode in Java Document
                                      (.build)))

(update-defaults :client (MongoClients/create ^MongoClientSettings (defaults :client-settings)))

(try (drop-collection :testdb.testcoll) (catch Exception e ""))

;;10k documents
;;[{a: 0 , b :[...140numbers...]}
; {a: 1 , b : [...140numbers...]}
; ...
; {a :10000, b : [...140numbers...]}]
;And then i do this
;
;aggregate(
;[{"$group": {"_id": "$a", "docs": {"$push": "$$ROOT"}}}])

;;https://www.mongodb.com/community/forums/t/group-memory-limit/134688

(defn add-docs [ndocs start]
  (loop [n start
         docs []]
    (if (= n (+ ndocs start))
      (insert :testdb.testcoll docs)
      (recur (inc n) (conj docs {:a n  :b (range 200)})))))

(dotimes [x 10] (add-docs 1000 (* x 1000)))

#_(c-print-all (.allowDiskUse ^AggregateIterable
                            (q :testdb.testcoll
                               (group {:_id :a}
                                      {:docs (conj-each :ROOT.)})
                               {:docs (count :docs)}
                               )
                            false))

(pprint (q :testdb.testcoll
           (group {:_id :a}
                  {:docs (conj-each :ROOT.)})
           {:docs (count :docs)}
           (command)))

#_(try (drop-collection :testdb.testcoll) (catch Exception e ""))

#_(defn add-docs [ndocs start]
  (loop [n start
         docs []]
    (if (= n (+ ndocs start))
      (insert :testdb.testcoll docs)
      (recur (inc n) (conj docs {:a n  :b (range 20000)})))))

#_(dotimes [x 1] (add-docs 100 (* x 100)))

#_(c-print-all (.allowDiskUse ^AggregateIterable
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



