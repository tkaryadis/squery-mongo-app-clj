(ns cmql-app-clj.mongodb-forum.forum4-unwind-group
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
           (com.mongodb MongoClientSettings)))


;;https://developer.mongodb.com/community/forums/t/aggregate-querry-with-array-field/113765/3

(update-defaults :client-settings (-> (MongoClientSettings/builder)
                                      (.codecRegistry clj-registry) ;;Remove this if you want to decode in Java Document
                                      (.build)))

(update-defaults :client (MongoClients/create ^MongoClientSettings (defaults :client-settings)))

(try (drop-collection :testdb.testcoll) (catch Exception e ""))

(def docs [
           {
            "_id" {
                   "oid" "60db82fbce48b83d522bf2f4"
                   },
            "name" "AS1",
            "rms" [
                   {
                    "name" "rms1",
                    "_id" "rmsid1"
                    },
                   {
                    "name" "rms2",
                    "_id" "rmsid2"
                    },
                   ]
            }

           {
            "_id" {
                   "oid" "60db82fbce48b83d522bf2f5"
                   },
            "name" "AS2",
            "rms" [
                   {
                    "name" "rms1",
                    "_id" "rmsid1"
                    },
                   {
                    "name" "rms4",
                    "_id" "rmsid4"
                    }
                   ]
            }
           ])

(insert :testdb.testcoll docs)

(c-print-all (q :testdb.testcoll
                (unwind :rms)
                (group {:_id {:name :rms.name
                              :_id  :rms._id}})
                (replace-root :_id)
                {:print true}))