(ns cmql-app-clj.cmql.stages.lookup
  (:refer-clojure :only [])
  (:use cmql-core.operators.operators
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