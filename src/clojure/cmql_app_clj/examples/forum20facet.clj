(ns cmql-app-clj.examples.forum20facet
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

(update-defaults :client-settings (-> (MongoClientSettings/builder)
                                      (.codecRegistry clj-registry)
                                      (.build)))

(update-defaults :client (MongoClients/create ^MongoClientSettings (defaults :client-settings)))


(try (drop-collection :testdb.testcoll) (catch Exception e ""))

;;https://www.mongodb.com/community/forums/t/how-to-compute-frequency-for-multiple-fields-using-a-single-pipeline-in-mongodb/118808/3

(insert :testdb.testcoll
        [
         {
          "_id" {
                 "field1" "value1",
                 "field2" "v1"
                 },
          "count" 7.0
          },
         {
          "_id" {
                 "field1" "value1",
                 "field2" "v2"
                 },
          "count" 3.0
          },
         {
          "_id" {
                 "field1" "value2",
                 "field2" "v1"
                 },
          "count" 4.0
          }])

;;Wanted result
;;{
;  "field1": [
;    "value1": 10.0,
;    "value2": 4.0
;  ],
;  "field2": [
;    "v1": 11.0,
;    "v2": 3.0
;  ]
;}
(c-print-all (q :testdb.testcoll
                (facet {:field1 [[:_id.field1 :count]
                                 [:!_id {:value :_id.field1} :count]
                                 (group :value
                                        {:count (sum :count)})]
                        :field2 [[:_id.field2 :count]
                                 [:!_id {:value :_id.field2} :count]
                                 (group :value
                                        {:count (sum :count)})]})))