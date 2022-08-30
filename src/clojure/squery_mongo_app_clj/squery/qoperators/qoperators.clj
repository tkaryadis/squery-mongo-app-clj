(ns squery-mongo-app-clj.squery.qoperators.qoperators
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

(insert :testdb.testcoll [{"name" "a", "custom" true, "users" [{"_id" 1} {"_id" 2} {"_id" 3}]}
                          {"name" "a", "custom" true, "users" [{"_id" 1} {"_id" 3}]}
                          {"name" "a", "custom" false, "users" [{"_id" 1} {"_id" 3}]}])


(pprint
  (q :testdb.testcoll
     (not? :custom (=? true))
     (command)
     ))

(c-print-all
  (q :testdb.testcoll
     (or? (not? :custom (=? true))
          (and? (=? :custom true) (superset? :users._id [2])))
     ;(command)
     ))

;;aggregate(
;          {"$match"
;           {"$or"
;            [{"$not" "$custom"}
;             {"$and" [{"custom" {"$eq" true}} {"users._id" {"$all" [2]}}]}]}})

#_(pprint (q :testdb.testcoll
           (q= :a 1)
           (q> :a 0)
           (= :a 1)
           (qor (q> :a 0)
                (q> :a 1))
           (command)))

;;db.scores.find(
;   { results: { $elemMatch: { $gte: 80, $lt: 85 } } }
;)

;(prn (elem-match :results (q>= 80) (q< 85)))

#_(pprint (fq :testdb.testcoll
            (elem-match :results (q>= 80) (q< 85))
            (command)))

#_(c-print-all (q :testdb.testcoll
                (elem-match :results (q>= 80) (q< 85))
                 ;(elem-match :results (q>= 80) (q< 85))
                ))

#_(prn (qf {:_id (q= 1)
          :a (q> 2)
          :c 2}))

#_(pprint (q :testdb.testcoll
           (elem-match :results (q>= 80) (q< 85))
           (qf {:_id (q= 1)
                :a 1})
           ;(elem-match :results (q>= 80) (q< 85))
           (command)))

#_(pprint (q :testdb.testcoll
           ;(elem-match :results (q>= 80) (q< 85))
           ;(elem-match :results (q>= 80) (q< 85))
           (qf {:_id (q= 1) :a 1})
           ;(= :_id 1)
            (command)))

#_(pprint (fq :testdb.testcoll
            ;(elem-match :results (q>= 80) (q< 85))
            ;(elem-match :results (q>= 80) (q< 85))
            ;(qf {:_id (q= 1) :a 1})
            ;(= :_id 1)
            (command)))

#_(def coll (.getCollection (.getDatabase (defaults :client) "testdb") "testcoll"))

#_(prn (f (elem-match :results (q>= 80) (q< 85))
        (elem-match :results (q>= 80) (q< 85))
        (= :_id 1)))

#_(c-print-all (.find coll
                    (f (elem-match :results (q>= 80) (q< 85))
                       (elem-match :results (q>= 80) (q< 85))
                       (= :_id 1))))