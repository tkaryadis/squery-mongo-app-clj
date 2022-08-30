(ns squery-mongo-app-clj.examples.forum8-group-distinct
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

;;https://developer.mongodb.com/community/forums/t/help-creating-group-and-count-query/9270/10

(update-defaults :client-settings (-> (MongoClientSettings/builder)
                                      (.codecRegistry clj-registry) ;;Remove this if you want to decode in Java Document
                                      (.build)))

(update-defaults :client (MongoClients/create ^MongoClientSettings (defaults :client-settings)))

(try (drop-collection :testdb.testcoll) (catch Exception e ""))


(def docs [{"program_name" "E"  "user_name" "B"   "zipfile_dt" "2019-01-01"}
           {"program_name" nil  "user_name" "B"   "zipfile_dt" "2020-01-01"}
           {"program_name" "E"  "user_name" "B"   "zipfile_dt" "2020-01-01"}
           {"program_name" "C"  "user_name" "B"   "zipfile_dt" "2020-01-01"}
           {"program_name" "D"  "user_name" "A"   "zipfile_dt" "2020-01-01"}
           {"program_name" "D"  "user_name" "A"   "zipfile_dt" "2020-01-01"}
           {"program_name" "D"  "user_name" "A"   "zipfile_dt" "2020-01-01"}])

(insert :testdb.testcoll docs)

(c-print-all (q :testdb.testcoll
                (= :zipfile_dt "2020-01-01")
                (group nil
                       {
                        :program_name_array (conj-each-distinct :program_name)
                        :user_name_array (conj-each-distinct :user_name)
                        })
                [{:users_count (count (filter (fn [:m.]
                                                (not (= :m. nil)))
                                              :user_name_array))}
                 {:program_count (count (filter (fn [:m.]
                                                  (not (= :m. nil)))
                                                :program_name_array))}]))
