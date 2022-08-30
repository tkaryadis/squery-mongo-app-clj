(ns squery-mongo-app-clj.examples.ex40-index-timeseries
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

(create-collection :testdb.testcoll
                   {
                    "timeseries"  {
                                   "timeField" "ts",
                                   "metaField" "md"
                                   }
                    })

(defn add-docs [ndocs start]
  (loop [n start
         docs []]
    (if (= n (+ ndocs start))
      (insert :testdb.testcoll docs)
      (recur (inc n) (conj docs {
                                 "ts" (ISODate)
                                 "md" {
                                       "id" (str n)
                                      },
                                 "value" 10
                                 })))))

(dotimes [x 2000] (add-docs 1000 (* x 1000)))


(System/exit 0)
;(create-index :testdb.testcoll (index [:ts]))

#_(c-print-all (.hint ^FindIterable (fq :testdb.testcoll
                                      (sort :ts)
                                      (limit 1))
                    "ts_1"))

;(System/exit 0)

#_(pprint (.explain (q :testdb.testcoll
                     (sort :ts)
                     (limit 1)
                     [:ts])))

;;1999000
;;"2021-12-20T15:20:21.243-00:00"
(c-print-all (q :testdb.testcoll
                (= :ts (ISODate "2021-12-20T15:20:21.243Z"))
                ;(sort :!ts)
                (count-s)
                ;(limit 1)
                ))

#_(time (c-take-all (q :testdb.testcoll
                     (sort :!ts)
                     (limit 1))))

#_(time (c-take-all (q :testdb.testcoll
                     (sort :ts)
                     (limit 1))))

#_(time (c-take-all (fq :testdb.testcoll
                      (sort :ts)
                      (limit 1))))