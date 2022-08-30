(ns squery-mongo-app-clj.squery.commands.read_write.t5find-and-modify
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


;;--------------------------insert--------------------------------------------------------

(def docs [{
            "_id"  1
            "name"  "MongoDB"
            "type"  "database"
            "versions" [ "v3.2" "v3.0" "v2.6" ]
            "info" { "x"  203 "y"  102 }
            }
           {
            "_id"  2
            "name"  "MongoDB"
            "type"  "database"
            "versions" [ "v3.2" "v3.0" "v2.6" ]
            "info" { "x"  203 "y"  102 }
            }
           {
            "_id"  3
            "name"  "MongoDB"
            "type"  "databases"
            "versions" [ "v3.2" "v3.0" "v2.6" ]
            "info" { "x"  203 "y"  102 }
            }])

(insert :testdb.testcoll docs)

;;--------------------------updateOne--------------------------------------------------------

;;print all document of the collection,including the one we inserted
(println "----------After Insert------------")
(c-print-all (q :testdb.testcoll))


(def modified (find-and-modify :testdb.testcoll
                               (= :_id 3)
                               {:name (str :name "-with-id=3")}
                               (fields-o :!_id :name)
                               (new-o)))

(println "----------Returned doc------------")
(prn (get modified :value))

(def modified (find-and-modify :testdb.testcoll
                               (= :name "MongoDB")
                               {:name (str :name "-with-id=1 (many found,but because sort,id=1 was choosen)")}
                               (sort :_id)
                               (fields-o :!_id :name)
                               (new-o)))

(println "----------Returned doc------------")
(prn (get modified :value))

(def modified-upserted (find-and-modify :testdb.testcoll
                                        (upsert {:_id 4})
                                        {:name (if- (exists? :name)
                                                    (str :name "-with-id=4")
                                                    "new-name")}
                                        (fields-o :!_id :name)
                                        (new-o)))

(println "----------Returned doc upserted------------")
(prn (get modified-upserted :value))

(println "----------After find-and-modify------------")
(c-print-all (q :testdb.testcoll))

;;---------------------------update-operators(not pipeline)-------------------------------------------------------------

(def modified-upserted (find-and-modify :testdb.testcoll
                                        (upsert {:_id 5})
                                        (set!- :name "name-with-update-operator-set")
                                        (fields-o :!_id :name)
                                        (new-o)))

(println "----------Returned doc upserted------------")
(prn (get modified-upserted :value))

(println "----------After find-and-modify------------")
(c-print-all (q :testdb.testcoll))


;;---------------------------replace document including the _id is not allowed => ERROR-----

(def modified-upserted (find-and-modify :testdb.testcoll
                                        (=? :_id 5)
                                        {:update {:_id 100}}))

(println "----------After find-and-modify------------")
(c-print-all (q :testdb.testcoll))


