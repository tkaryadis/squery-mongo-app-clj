(ns squery-mongo-app-clj.examples.forum25pushIntoSortedv2
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


(try (drop-collection :testdb.testcoll) (catch Exception e ""))

(insert :testdb.testcoll [{"location_id" 1,
                           "probes" [{"probe_id" 2,
                                      "readings" [{"value" 42, "when" 5}
                                                  {"value" 37, "when" 10}
                                                  {"value" 43, "when" 15}
                                                  {"value" 41, "when" 20}]}]}])


(defn query [new-member message]
  (println "Case :" message "Member :" new-member)
  (c-print-all
    (q :testdb.testcoll
       (=- :location_id 1)
       {:prob-id (if- (empty? :probes)
                   4
                   2)}
       {:probes (if- (empty? :probes)
                  [{"probe_id" :prob-id
                    "readings" []}]
                  :probes)}
       {:probes
        (map (fn [:m1.]
               (if- (not= :m1.probe_id. :prob-id)
                 :m1.
                 (merge :m1.
                        {:readings
                         (let [:size-position.
                               (reduce (fn [:index-pos. :m2.]
                                         (let [:index. (get :index-pos. 0)
                                               :pos. (get :index-pos. 1)]
                                           (if- (and (nil? :pos.) (> :m2.when. (c/get new-member "when")))
                                             [(inc :index.) :index.]
                                             [(inc :index.) :pos.])))
                                       [0 nil nil]
                                       :m1.readings.)
                               :asize. (get :size-position. 0)
                               :position. (get :size-position. 1)]
                           ;[:position. :prv-member.]
                           (cond

                             (= :asize. 0)
                             [new-member]

                             (nil? :position.)                ;;add to the end
                             (let [:prv-member. (get :m1.readings. {:index (dec :asize.)})]
                               (if- (= :prv-member.value. (c/get new-member "value"))
                                 :m1.readings.
                                 (concat :m1.readings. [new-member])))

                             (= :position. 0)                 ;;add at start
                             (let [:next-member. (get :m1.readings. 0)]
                               (if- (= :next-member.value. (c/get new-member "value"))
                                 (if- (< (c/get new-member "when") :next-member.when.)
                                   (concat [new-member] (take 1 :asize. :m1.readings.))
                                   :m1.readings.)
                                 (concat [new-member] :m1.readings.)))

                             :else                           ;;add in the middle
                             (let [:next-member. (get :m1.readings. {:index :position.})
                                   :prv-member. (get :m1.readings. {:index (dec :position.)})]
                               (cond
                                 (and (not= :next-member.value. (c/get new-member "value"))
                                      (not= :prv-member.value. (c/get new-member "value")))
                                 (concat (take 0 :position. :m1.readings.)
                                         [new-member]
                                         (take :position. (inc :asize.) :m1.readings.))

                                 (= :prv-member.value. (c/get new-member "value"))
                                 :m1.readings.

                                 :else
                                 (concat (take 0 :position. :m1.readings.)
                                         [new-member]
                                         (take (inc :position.) (inc :asize.) :m1.readings.))))))})))
             :probes)}
       (unset :prob-id))))


[{"value" 42, "when" 5}
 {"value" 37, "when" 10}
 {"value" 43, "when" 15}
 {"value" 41, "when" 20}]

(println "---------------------Empty probes-------------------------------")
(println "Before add")
(c-print-all (q :testdb.testcoll))

(query {"value" 41 "when" 3} "Only 1 case exists[empty list - Add item(the bonus case)]")

(println "---------------------Empty readings-------------------------------")
(println "Before add")
(c-print-all (q :testdb.testcoll))

(query {"value" 41 "when" 3} "Only 1 case exists[empty list - Add item]")


;push middle, different value from preceding and following** member - Add item
;push middle, same value as preceding member - Do nothing (don't push)
;push middle, same value as following member - Replace following member


(println "---------------------Not empty readings-------------------------------")
(println "Before add")
(c-print-all (q :testdb.testcoll))

(query {"value" 41 "when" 3} "NoConflict start[push front, different value from following member - Add item]")
(query {"value" 42 "when" 3} "Conflict start[push front, same value as following member - Replace following member]")

[{"value" 42, "when" 5}
 {"value" 37, "when" 10}
 {"value" 43, "when" 15}
 {"value" 41, "when" 20}]

(query {"value" 42 "when" 11} "NoConflict middle[push middle, different value from preceding and following** member - Add item]")
(query {"value" 37 "when" 11} "Conflict middle[push middle, same value as preceding member - Do nothing (don't push)]")
(query {"value" 43 "when" 11} "Conflict middle[push middle, same value as following member - Replace following member]")

(query {"value" 47 "when" 21} "NoConflict end[push back, different value from preceding* member - Add item]")
(query {"value" 41 "when" 21} "Conflict end[push back, same value as preceding member - Do nothing (don't push)]")

;;---------------------------------------------------------------------------------------------------------------

(def new-member {"value" 43 "when" 11})

(c-print-all (q :testdb.testcoll))

(println (clj->json (update- :testdb.testcoll
                      (uq (=- :location_id 1)
                          {:prob-id (if- (empty? :probes)
                                      4
                                      2)}
                          {:probes (if- (empty? :probes)
                                     [{"probe_id" :prob-id
                                       "readings" []}]
                                     :probes)}
                          {:probes
                           (map (fn [:m1.]
                                  (if- (not= :m1.probe_id. :prob-id)
                                    :m1.
                                    (merge :m1.
                                           {:readings
                                            (let [:size-position.
                                                  (reduce (fn [:index-pos. :m2.]
                                                            (let [:index. (get :index-pos. 0)
                                                                  :pos. (get :index-pos. 1)]
                                                              (if- (and (nil? :pos.) (> :m2.when. (c/get new-member "when")))
                                                                [(inc :index.) :index.]
                                                                [(inc :index.) :pos.])))
                                                          [0 nil nil]
                                                          :m1.readings.)
                                                  :asize. (get :size-position. 0)
                                                  :position. (get :size-position. 1)]
                                              ;[:position. :prv-member.]
                                              (cond

                                                (= :asize. 0)
                                                [new-member]

                                                (nil? :position.)                ;;add to the end
                                                (let [:prv-member. (get :m1.readings. {:index (dec :asize.)})]
                                                  (if- (= :prv-member.value. (c/get new-member "value"))
                                                    :m1.readings.
                                                    (concat :m1.readings. [new-member])))

                                                (= :position. 0)                 ;;add at start
                                                (let [:next-member. (get :m1.readings. 0)]
                                                  (if- (= :next-member.value. (c/get new-member "value"))
                                                    (if- (< (c/get new-member "when") :next-member.when.)
                                                      (concat [new-member] (take 1 :asize. :m1.readings.))
                                                      :m1.readings.)
                                                    (concat [new-member] :m1.readings.)))

                                                ;;;push middle, different value from preceding and following** member - Add item
                                                ;;push middle, same value as preceding member - Do nothing (don't push)
                                                ;;push middle, same value as following member - Replace following member

                                                :else                           ;;add in the middle
                                                (let [:next-member. (get :m1.readings. {:index :position.})
                                                      :prv-member. (get :m1.readings. {:index (dec :position.)})]
                                                  (cond
                                                    (and (not= :next-member.value. (c/get new-member "value"))
                                                         (not= :prv-member.value. (c/get new-member "value")))
                                                    (concat (take 0 :position. :m1.readings.)
                                                            [new-member]
                                                            (take :position. (inc :asize.) :m1.readings.))

                                                    (= :prv-member.value. (c/get new-member "value"))
                                                    :m1.readings.

                                                    :else
                                                    (concat (take 0 :position. :m1.readings.)
                                                            [new-member]
                                                            (take (inc :position.) (inc :asize.) :m1.readings.))))))})))
                                :probes)}
                          (unset :prob-id))
                      {:command true})))

#_(c-print-all (q :testdb.testcoll))