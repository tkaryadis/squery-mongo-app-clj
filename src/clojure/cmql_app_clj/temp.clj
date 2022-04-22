(ns cmql-app-clj.temp
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

(insert :testdb.testcoll [{"status" "PARTIALLY_REFUNDED",
                           "amount" 100,
                           "FIELD_B" 20,
                           "FIELD_A" 10}
                          {"status" "PARTIALLY_REFUNDED", "FIELD_B" 20, "FIELD_A" 10}])

(prn "BEFORE")
(c-print-all (q :testdb.testcoll ))

(prn (update- :testdb.testcoll
              (uq (=- :status "PARTIALLY_REFUNDED")
                  (exists?- :amount)
                  {:amountRefundRemaining (- :FIELD_B :FIELD_A)}  )))

(prn "AFTER")
(c-print-all (q :testdb.testcoll))

#_(pprint (update- :testdb.testcoll
                   (uq (upsert {:a 4})
                       (+_ :a 1))
                   (command)))
