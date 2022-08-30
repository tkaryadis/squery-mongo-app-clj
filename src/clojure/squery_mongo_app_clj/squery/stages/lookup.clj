(ns squery-mongo-app-clj.squery.stages.lookup
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

(pprint (q :testdb.testcoll
           (lookup :a :2.b :joined)
           (command)))

(pprint (q :testdb.testcoll
           (lookup :a.d :2.b.c :joined)
           (command)))

(pprint (q :testdb.testcoll
           (lookup-p :2
                     [:pa. :a]
                     [(= :pa. :afield)]
                     :joined)
           (command)))

(pprint (q :testdb.testcoll
           (lookup-p [:a :2.b]
                     [:pc. :c]
                     [(= :pc. :d)]
                     :joined)
           (command)))

(pprint (q :testdb.testcoll
           (lookup-p [:a.b :2.c.d]
                     [:pe. :e]
                     [(= :pf. :g)]
                     :joined)
           (command)))