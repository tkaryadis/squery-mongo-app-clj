(ns cmql-app-clj.examples.forum16arraysintersection
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

;;https://www.mongodb.com/community/forums/t/pull-or-pullall-to-delete-array-of-emails/117503

(update-defaults :client-settings (-> (MongoClientSettings/builder)
                                      (.codecRegistry clj-registry) ;;Remove this if you want to decode in Java Document
                                      (.build)))

(update-defaults :client (MongoClients/create ^MongoClientSettings (defaults :client-settings)))

(try (drop-collection :testdb.testcoll) (catch Exception e ""))

(def doc {"ar" [1 2 3 4]})

(insert :testdb.testcoll doc)

#_(c-print-all (q :testdb.testcoll
                (match {"ar" {"$in" [1 2 5]}})))

#_(c-print-all (q :testdb.testcoll
                (match {"$expr" {"$in" [[2 5] [2 5]]}})))