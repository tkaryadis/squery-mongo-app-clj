(ns cmql-app-clj.cmql.commands.read_write.t6aggregation
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

(try (drop-collection :testdb.mytypes) (catch Exception e ""))
(try (drop-collection :testdb.people) (catch Exception e ""))
(try (drop-collection :testdb.pages) (catch Exception e ""))

(insert :testdb.mytypes
        [{ :_id 1
          :myint 10
          :mydouble 0.50
          :mybool true
          :mystring "gggddeee"
          :myarray [1 2 5 10]
          :myobject1 {:a 2}}
         { :_id 2
          :myint 10
          :mydouble 0.50
          :mybool true
          :mystring "gggddeee"
          :myarray [1 2 5 11]
          :myobject1 {:a 2}}])

;;;;{ $reverseArray: { $literal: [ 1, 2, 3 ] } }


(c-print-all (q :testdb.mytypes
                      [{:name (ziparray [1 2 ] [1 2 3 4 5 6 7] {:defaults [5 2]})}]
                      ))



;;-----------------------------example2----------------------------------------------
;;-----------------------------------------------------------------------------------

;;Example from https://docs.mongodb.com/manual/reference/operator/aggregation/reduce/

(insert :testdb.people
        [{ "_id"  1, "name"  "Melissa", "hobbies"  [ "softball", "drawing", "reading" ] }
         { "_id"  2, "name"  "Brad", "hobbies"  [ "gaming", "skateboarding" ] }
         { "_id"  3, "name"  "Scott", "hobbies"  [ "basketball", "music", "fishing" ] }
         { "_id"  4, "name"  "Tracey", "hobbies"  [ "acting", "yoga" ] }
         { "_id"  5, "name"  "Josh", "hobbies"  [ "programming" ] }
         { "_id"  6, "name"  "Claire" }])



(c-print-all
  (q :testdb.people
     (array? :hobbies)
     (> (count :hobbies) 0)
     [:name
      {:bio (reduce (fn [:hobbies. :hobbie.]
                          (str :hobbie. (if- (= :hobbie. "My hobbies include:") " " ", ") :hobbies.))
                     "My hobbies include:"
                     :hobbies)}]))



;;---------------------------------example3--------------------------------------------
;;https://docs.mongodb.com/manual/reference/operator/aggregation/zip/

;;db.pages.aggregate([{
;  $project: {
;    _id: false,
;    pages: {
;      $filter: {
;        input: {
;          $zip: {
;            inputs: [ "$pages", { $range: [0, { $size: "$pages" }] } ]
;          }
;        },
;        as: "pageWithIndex",
;        cond: {
;          $let: {
;            vars: {
;              page: { $arrayElemAt: [ "$$pageWithIndex", 0 ] }
;            },
;            in: { $gte: [ "$$page.reviews", 1 ] }
;          }
;        }
;      }
;    }
;  }
;}])

(insert :testdb.pages
        {
         "category" "unix"
         "pages" [
                  { "title" "awk for beginners", "reviews"  5 }
                  { "title" "sed for newbies", "reviews" 0 },
                  { "title" "grep made simple", "reviews" 2 },
                  ]
         })

(c-print-all
  (q :testdb.pages
     [:!_id {:pages (filter (fn [:pageWithIndex.]
                                  (let [:page. (get :pageWithIndex. 0)]
                                        (>= :page.reviews. 1)))
                             (ziparray :pages (range 0 (count :pages))))}]
     {:allowDiskUse	true
      :maxTimeMS 10000}
     {"comment" ""}))