(ns cmql-app-clj.examples.forum26pushIntoSortedv3
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
        flatland.ordered.map
        clojure.pprint)
  (:refer-clojure)
  (:require [clojure.core :as c]
            [clojure.data.json :as tojson])
  (:import (com.mongodb.client MongoClients MongoCollection MongoClient)
           (com.mongodb MongoClientSettings)
           (org.bson.types ObjectId)
           (java.util Date Calendar)))

(update-defaults :client-settings (-> (MongoClientSettings/builder)
                                      ;(.codecRegistry clj-registry)
                                      (.build)))

(update-defaults :client (MongoClients/create ^MongoClientSettings (defaults :client-settings)))


(defn init []
  (try (drop-collection :testdb.testcoll) (catch Exception e ""))
  (try (drop-collection :testdb.testcoll1) (catch Exception e ""))

  (create-index :testdb.testcoll (index [:location_id] {:unique true}))

  (insert :testdb.testcoll1 [{}])

  (insert :testdb.testcoll [{"location_id" 1,
                             "probes" [{"probe_id" 2,
                                        "readings" [{"value" 42, "when" 5}
                                                    {"value" 37, "when" 10}
                                                    {"value" 43, "when" 15}
                                                    {"value" 41, "when" 20}]}
                                       #_{"probe_id" 3,
                                        "readings" [{"value" 42, "when" 5}
                                                    {"value" 37, "when" 10}
                                                    {"value" 43, "when" 15}
                                                    {"value" 41, "when" 20}]}]}]))

(defn query [new-member message]
  (println "Case :" message "Member :" new-member)
  (init)
  (.toCollection (q :testdb.testcoll
                    (=- :location_id 1)
                    {:prob-id (if- (empty? :probes)
                                4
                                2)}
                    {:probes (if- (empty? :probes)
                               [{"probe_id" :prob-id
                                 "readings" []}]
                               :probes)}
                    (lookup-p :testcoll1
                              [:probes. :probes]
                              [{:probes (conj :probes. new-member)}
                               (unwind :probes)
                               (unwind :probes.readings)
                               (sort :probes.probe_id :probes.readings.when)
                               (replace-root :probes)]
                              :probes)
                    {:probes (reduce (fn [:ps. :p.]
                                       (let [:prv-p. (last :ps.)]
                                         (if- (and :prv-p.
                                                   (= :prv-p.readings.value.
                                                      :p.readings.value.)
                                                   (= :prv-p.probe_id.
                                                      :p.probe_id.)
                                                   (= :p.probe_id. (c/get new-member :probe_id)))
                                           :ps.
                                           (conj :ps. :p.))))
                                     []
                                     :probes)}
                    (lookup-p :testcoll1
                              [:probes. :probes]
                              [{:probes :probes.}
                               (unwind-replace-root :probes)
                               (group :probe_id
                                      {:readings (conj-each :readings)})]
                              :probes)
                    (unset :_id :prob-id)
                    (merge-s :testdb.testcoll
                             (if-match [:location_id]
                                       "replace"
                                       "discard"))))
  (c-print-all (q :testdb.testcoll)))


(comment
(println "---------------------Empty probes-------------------------------")
(println "Before add")
(init)
(c-print-all (q :testdb.testcoll))

(query {:probe_id 2 :readings [{"value" 42 "when" 3}]} "Only 1 case exists[empty list - Add item(the bonus case)]")

)

(comment
(println "---------------------Empty readings-------------------------------")
(println "Before add")
(init)
(c-print-all (q :testdb.testcoll))

(query {:probe_id 2 :readings [{"value" 42 "when" 3}]} "Only 1 case exists[empty list - Add item]")
)

(comment
(println "---------------------Not empty readings-------------------------------")
(println "Before add")
(c-print-all (q :testdb.testcoll))

(query {:probe_id 2 :readings [{"value" 41 "when" 3}]} "NoConflict start[push front, different value from following member - Add item]")
(query {:probe_id 2 :readings [{"value" 42 "when" 3}]} "Conflict start[push front, same value as following member - Replace following member]")

(query {:probe_id 2 :readings [{"value" 42 "when" 11}]} "NoConflict middle[push middle, different value from preceding and following** member - Add item]")
(query {:probe_id 2 :readings [{"value" 37 "when" 11}]} "Conflict middle[push middle, same value as preceding member - Do nothing (don't push)]")
(query {:probe_id 2 :readings [{"value" 43 "when" 11}]} "Conflict middle[push middle, same value as following member - Replace following member]")

(query {:probe_id 2 :readings [{"value" 47 "when" 21}]} "NoConflict end[push back, different value from preceding* member - Add item]")
(query {:probe_id 2 :readings [{"value" 41 "when" 21}]} "Conflict end[push back, same value as preceding member - Do nothing (don't push)]")
)
;;---------------------------------------------------------------------------------------------------------------

(def new-member {:probe_id 2 :readings [{"value" 43 "when" 11}]})

;(c-print-all (q :testdb.testcoll))

(println (clj->json (q :testdb.testcoll
                       (=- :location_id 1)
                       {:prob-id (if- (empty? :probes)
                                   4
                                   2)}
                       {:probes (if- (empty? :probes)
                                  [{"probe_id" :prob-id
                                    "readings" []}]
                                  :probes)}
                       (lookup-p :testcoll1
                                 [:probes. :probes]
                                 [{:probes (conj :probes. new-member)}
                                  (unwind :probes)
                                  (unwind :probes.readings)
                                  (sort :probes.probe_id :probes.readings.when)
                                  (replace-root :probes)]
                                 :probes)
                       {:probes (reduce (fn [:ps. :p.]
                                          (let [:prv-p. (last :ps.)]
                                            (if- (and :prv-p.
                                                      (= :prv-p.readings.value.
                                                         :p.readings.value.)
                                                      (= :prv-p.probe_id.
                                                         :p.probe_id.)
                                                      (= :p.probe_id. (c/get new-member :probe_id)))
                                              :ps.
                                              (conj :ps. :p.))))
                                        []
                                        :probes)}
                       (lookup-p :testcoll1
                                 [:probes. :probes]
                                 [{:probes :probes.}
                                  (unwind-replace-root :probes)
                                  (group :probe_id
                                         {:readings (conj-each :readings)})]
                                 :probes)
                       (unset :_id :prob-id)
                       (merge-s :testdb.testcoll
                                (if-match [:location_id]
                                          "replace"
                                          "discard"))
                       {:command true})))

#_(c-print-all (q :testdb.testcoll))