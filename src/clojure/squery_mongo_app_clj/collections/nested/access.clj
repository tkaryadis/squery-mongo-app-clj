(ns squery-mongo-app-clj.collections.nested.access
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

;;Reason
;;MongoDB
;;1)stage operators (addfields,unset,project) and  . for the path
;;  path is very limited
;;  not mixed,no variables,no conditions
;;  this means that its only for top level,or nested documents where
;;  all parents are documents
;;  for example i can do (addFields {:mydoc1.mydoc2.h 5})
;;  but its very limited (addFields {:mydoc.2.$$x.h 5}) doesnt work
;;2)aggregate operators
;;  using those we can do them but its hard and requires lot of code
;;  with $map,$mergeObjects,$ObjectToArray,$ArrayToObject etc


(update-defaults :client-settings (-> (MongoClientSettings/builder) (.codecRegistry clj-registry) (.build)))
(update-defaults :client (MongoClients/create ^MongoClientSettings (defaults :client-settings)))

(try (drop-collection :testdb.testcoll) (catch Exception e ""))

;;get-          arrays/objects 0 level only
;;get-in-       nested arrays/objects any mix

(def nested-doc {:myarray  [1 [2 [3 4 5] 6] 7]
                 :myobject {:a "b" :c {:d "e" :e "f"}}
                 :mymixedobject {:a "b" :c {:d [1 2 3]}}
                 :mymixedarray [1 {:a "b" :c {:d [1 2 3]}}]
                 })

(insert :testdb.testcoll nested-doc)

(def c
  (q :testdb.testcoll
     {
      ;; array
      :v0 (= (get :myarray 1) [2 [3 4 5] 6])
      :v1 (= (get-in :myarray [1]) [2 [3 4 5] 6])
      :v2 (= (get-in :myarray [1 1]) [3 4 5])
      :v3 (= (get-in :myarray [1
                             {
                              :icond (array? :v.)           ;;icond or index not both
                              :cond  (array? :a.)
                              }
                             2
                             ])
           5)

      ;; :myobject {:a "b" :c {:d "e" :e "f"}}
      :v4 (= (get :myobject "c") {:d "e" :e "f"})
      :v5 (= (get-in :myobject ["c" "d"]) "e")
      :v6 (= (get-in :myobject ["c"
                              {:kcond (= :k. "e")
                               :cond  (object? :o.)}
                              ])
           "f")

      ;mixed only get-/get-in possible
      :v7 (= (get-in :mymixedobject ["c" "d" 1]) 2)
      :v8 (= (get-in :mymixedarray [1 "c" "d" 1]) 2)

      ;;[1 {:a "b" :c {:d [1 2 3]}}]

      :v9 (= (get-in :mymixedarray [
                                  {
                                   :cond  (object? (get :a. 1))
                                   :icond (object? :v.)
                                   }
                                  {
                                   :cond  (object? (get :o. "c"))
                                   :kcond (and (not= :k. "d")
                                               (not= :v. 1))
                                   }
                                  "d"
                                  1])
           2)
      }
     [:!_id :v0 :v1 :v2 :v3 :v4 :v5 :v6 :v7 :v8 :v9]
     ;(command)
     ))

(c-print-all c)