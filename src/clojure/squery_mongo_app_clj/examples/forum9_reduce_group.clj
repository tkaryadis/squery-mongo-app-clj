(ns squery-mongo-app-clj.examples.forum9-reduce-group
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


;;https://developer.mongodb.com/community/forums/t/updating-field-by-summing-values/9386/7

;;{
;    totalWins: 12,
;    seasons: {
;        1: {
;            wins: 10
;        },
;        2: {
;            wins: 2
;        }
;    }
;}

(connect)

(try (drop-collection :testdb.testcoll) (catch Exception e ""))

(def docs (json->clj "[{\n  \"_id\": {\n    \"oid\": \"5f765b65b9e3847a36ff10cb\"\n  },\n  \"name\": \"Xe\",\n  \"seasons\": [\n    {\n      \"season\": 0,\n      \"kills\": 1502,\n      \"wins\": 100\n    },\n    {\n      \"season\": -1,\n      \"kills\": 1,\n      \"wins\": 10\n    }\n  ]\n},{\n  \"_id\": {\n    \"oid\": \"5f765b66b9e3847a36ff10cc\"\n  },\n  \"name\": \"Rec\",\n  \"seasons\": [\n    {\n      \"season\": 0,\n      \"kills\": 1502,\n      \"wins\": 90\n    }\n  ]\n}]"
                     ))

(prn (insert :testdb.testcoll docs))

(prn (update_ :testdb.testcoll
              (uq (add {:totalwins (if- (exist? :seasons)
                                        (reduce- (fn- [:totalwins- :season-]
                                                      (+_ :totalwins- :season-.wins))
                                                 0
                                                 :seasons)
                                        0)}))
              ;(command)
              ))


;;{"globalTotalWins" 200}

(println (clj->json (q :testdb.testcoll
                       (group nil
                              {:globalTotalWins (sum- :totalwins)})
                       (command))))