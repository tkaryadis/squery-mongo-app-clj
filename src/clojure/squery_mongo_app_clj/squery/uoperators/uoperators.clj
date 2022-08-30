(ns squery-mongo-app-clj.squery.uoperators.uoperators
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

(insert :testdb.testcoll [{ "_id" 1, "a" 1,"results" [ 82, 85, 88 ] }
                          { "_id" 2, "a" 2, "results" [ 75, 88, 89 ] }])


(pprint (update- :testdb.testcoll
                 (uq (upsert {:a 4})
                     (+! :a 1))
                 (command)))

(c-print-all (q :testdb.testcoll))

;;;-------------------using interop and squery arguments-----------------------------------------------------------------

(try (drop-collection :testdb.testcoll) (catch Exception e ""))

(insert :testdb.testcoll [{ "_id" 1, "a" 1,"results" [ 82, 85, 88 ] }
                          { "_id" 2, "a" 2, "results" [ 75, 88, 89 ] }])


(def coll ^MongoCollection (.getCollection (.getDatabase (defaults :client) "testdb") "testcoll"))

(defn po [x] x)

(.updateOne coll
            (d {:a 2})
            (u (+! :a (po 100))
               (assoc! :results [1 2 3])))


(c-print-all (q :testdb.testcoll))

(drop-collection :testdb.testcoll)

(insert :testdb.testcoll [{"players" [{"id" 1234, "verses" [1 2 3]},
                                      {"id" 1235, "verses" [5 6 7]}]}])

(c-print-all (q :testdb.testcoll))

;;using one or more raw mql update inside a squery update with (mu ..)
(update- :testdb.testcoll
         (uq (mu { "$push" { "players.$[playerid].verses"  100 } }),
             { "arrayFilters" [ { "playerid.id" { "$eq" 1235 } } ] }))

(c-print-all (q :testdb.testcoll))