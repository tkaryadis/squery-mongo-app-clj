(ns cmql-app-clj.examples.forum13arraysvars
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
  (:import (com.mongodb.client MongoClients MongoCollection MongoDatabase MongoClient)
           (com.mongodb MongoClientSettings)))

;;https://www.mongodb.com/community/forums/t/mongodb-aggregation-lookup-key-value-based-on-another-key-value-within-an-array/116373

(update-defaults :client-settings (-> (MongoClientSettings/builder)
                                      (.codecRegistry clj-registry) ;;Remove this if you want to decode in Java Document
                                      (.build)))

(update-defaults :client (MongoClients/create ^MongoClientSettings (defaults :client-settings)))

(try (drop-collection :testdb.testcoll) (catch Exception e ""))

(def doc {"reference" "ReferenceStr",
          "subgroup" [{"#" 1, "param" 123} {"#" 2, "param" 456} {"#" 3, "param" 789}],
          "group" [{"#" 1, "start" 1, "end" 2} {"#" 2, "start" 3, "end" 3}]})

;;question= for group.# = 1 ,get group.end,and return the subgroup where  group.end==subgroup.#

(insert :testdb.testcoll doc)

(c-print-all (q :testdb.testcoll
                {:var (let [:group-end. (reduce (fn [:found-end. :g.]
                                                  (if- (and (= :found-end. nil) (= :g.#. 1))
                                                    :g.end.
                                                    :found-end.))
                                                nil
                                                :group)
                            :subgroup-param. (if- (= :group-end. nil)
                                               nil
                                               (reduce (fn [:found-param. :s.]
                                                         (if- (and (= :found-param. nil) (= :s.#. :group-end.))
                                                           :s.param.
                                                           :found-param.))
                                                       nil
                                                       :subgroup))]
                        :subgroup-param.)}
                [:!_id :reference :var]
                {:print 1}))

(c-print-all (q :testdb.testcoll
                {:found-group-end (let [:found-group. (get (filter (fn [:g.] (= :g.#. 1)) :group) 0)]
                                    :found-group.end.)}
                {:var (let [:found-subgroup. (get (filter (fn [:s.] (= :s.#. :found-group-end)) :subgroup) 0)]
                        :found-subgroup.param.)}
                [:!_id :reference {:var (if- (exists? :var) :var nil)}]
                {:print 1}))

(c-print-all (q :testdb.testcoll
                (unwind :group)
                (= :group.# 1)
                (unwind :subgroup)
                {:var (if- (= :subgroup.# :group.end)
                        :subgroup.param
                        nil)}
                (not= :var nil)
                [:!_id :reference :var]
                {:print 1}))