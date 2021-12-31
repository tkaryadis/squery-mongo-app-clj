(ns cmql-app-clj.examples.forum33indexfunction
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
  (:import (com.mongodb.client MongoClients MongoCollection MongoDatabase MongoClient)
           (com.mongodb MongoClientSettings)
           (java.sql Date)))


(update-defaults :client-settings (-> (MongoClientSettings/builder)
                                      (.codecRegistry clj-registry) ;;Remove this if you want to decode in Java Document
                                      (.build)))

(update-defaults :client (MongoClients/create ^MongoClientSettings (defaults :client-settings)))

#_(try (drop-collection :testdb.testcoll) (catch Exception e ""))

(defn add-docs [ndocs start]
  (loop [n start
         docs []]
    (if (= n (+ ndocs start))
      (insert :testdb.testcoll docs)
      (recur (inc n) (conj docs {:mydate (ISODate)})))))

#_(dotimes [x 100] (add-docs 10000 (* x 10000)))

#_(update- :testdb.testcoll
  (uq {:mydate (+ :mydate (* (rand) 1000000))}))

#_(insert :testdb.testcoll {:mydate (ISODate "2019-11-03T15:09:47.270Z")})

#_(create-index :testdb.testcoll (index [:mydate]))

(time (c-print-all (q :testdb.testcoll
                      (= :mydate (ISODate "2019-11-03T15:09:47.270Z")))))


;no index use because of function
(time (c-print-all (q :testdb.testcoll
                     (= (date-year :mydate)  2019))))