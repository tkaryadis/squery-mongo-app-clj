(ns cmql-app-clj.examples.forum38testSetPerfomance
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
  (:import (com.mongodb.client MongoClients MongoCollection MongoDatabase MongoClient AggregateIterable)
           (com.mongodb MongoClientSettings)
           (java.sql Date)))


(update-defaults :client-settings (-> (MongoClientSettings/builder)
                                      (.codecRegistry clj-registry) ;;Remove this if you want to decode in Java Document
                                      (.build)))

(update-defaults :client (MongoClients/create ^MongoClientSettings (defaults :client-settings)))

(try (drop-collection :testdb.testcoll) (catch Exception e ""))

(insert :testdb.testcoll {:ar1 (range 1000)})

(time (c-print-all (q :testdb.testcoll
                      {:ar2 (union (range 100000) [4 3 2])
                       :ar3 (union (range 2) [])}
                      {:c (count :ar2)}
                      {:b (count (map (fn [:this.]
                                        (contains? :ar3 :this.))
                                      :ar1))}
                      (unset :ar1 :ar2 :ar3))))

(time (c-print-all (q :testdb.testcoll
                      {:ar2 (union (range 100000) [4 3 2])
                       :ar3 (union (range 2) [])}
                      {:c (count :ar2)}
                      {:b (count (map (fn [:this.]
                                        (empty? (difference :ar2 [:this.])))
                                      :ar1))}
                      (unset :ar1 :ar2 :ar3))))