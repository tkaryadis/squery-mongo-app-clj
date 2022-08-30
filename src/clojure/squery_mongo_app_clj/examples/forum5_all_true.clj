(ns squery-mongo-app-clj.examples.forum5-all-true
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
  (:import (com.mongodb.client MongoClients MongoCollection MongoDatabase MongoClient)
           (com.mongodb MongoClientSettings)))


 ;;https://developer.mongodb.com/community/forums/t/find-with-string-greater-than/15435

 (update-defaults :client-settings (-> (MongoClientSettings/builder)
                                       (.codecRegistry clj-registry) ;;Remove this if you want to decode in Java Document
                                       (.build)))

 (update-defaults :client (MongoClients/create ^MongoClientSettings (defaults :client-settings)))

 (try (drop-collection :testdb.testcoll) (catch Exception e ""))




(insert :testdb.testcoll {:cpu "xeon" :disks [{:brand 1 :diskSizeGb "150"} {:brand 2 :diskSizeGb "250"}]})

(c-print-all (q :testdb.testcoll
                (all-true? (map (fn [:disk.] (> (int :disk.diskSizeGb.) 140)) :disks))))



