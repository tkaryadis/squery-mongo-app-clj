(ns squery-mongo-app-clj.examples.forum22pushIntoSorted
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

(update-defaults :client-settings (-> (MongoClientSettings/builder)
                                      ;(.codecRegistry clj-registry)
                                      (.build)))

(update-defaults :client (MongoClients/create ^MongoClientSettings (defaults :client-settings)))


(try (drop-collection :testdb.testcoll) (catch Exception e ""))

(insert :testdb.testcoll [{"_id" 1,
                           "array_1"
                                 [{"id" 2,
                                   "array_2"
                                   [{"value" 43, "when" 100}
                                         {"value" 49, "when" 80}
                                         {"value" 45, "when" 60}
                                         {"value" 41, "when" 20}]}]}])

(def new-member {"value" 41 "when" 10})


(c-print-all (q :testdb.testcoll))

#_(update- :testdb.testcoll
  (uq (=- :_id 1)
      {:array_1
       (map (fn [:m1.]
              (if- (not= :m1.id. 2)
                :m1.
                (merge :m1.
                       {:array_2
                        (let [:size-position.
                              (reduce (fn [:index-pos. :m2.]
                                        (let [:index. (get :index-pos. 0)
                                              :pos. (get :index-pos. 1)]
                                          (if- (< 50 :m2.when.)
                                            [(inc :index.) (inc :index.)]
                                            [(inc :index.) :pos.])))
                                      [0 -1]
                                      :m1.array_2.)
                              :asize. (get :size-position. 0)
                              :position. (get :size-position. 1)]
                          (cond

                            (= :position. -1)     ;;add one start
                            (cond
                              (= :asize 0)     ;;array was empty so i just add it
                              [{"value"  48 ,"when"  50}]

                              (let [:m3. (get :m1.array_2. 0)]
                                (= :m3.value. 48))
                              :m1.array_2.

                              :else
                              (concat [{"value"  48 ,"when"  50}] :m1.array_2.))

                            (= :position. :asize.)   ;;add at the end(i add anyways)
                            (concat :m1.array_2. [{"value"  48 ,"when"  50}])


                            :else                            ;;add in middle
                            (let [:next-member. (get :m1.array_2. {:index :position.})]
                              (if- (= :next-member.value. 48)
                                :m1.array_2.
                                (concat (take 0 :position. :m1.array_2.)
                                        [{"value"  48 ,"when"  50}]
                                        (take :position. :asize. :m1.array_2.))))))})))
            :array_1)}))

;(c-print-all (q :testdb.testcoll))

(c-print-all
  (q :testdb.testcoll
     (=- :_id 1)
     {:array_1
      (map (fn [:m1.]
             (if- (not= :m1.id. 2)
               :m1.
               (merge :m1.
                      {:array_2
                       (let [:size-position.
                             (reduce (fn [:index-pos. :m2.]
                                       (let [:index. (get :index-pos. 0)
                                             :pos. (get :index-pos. 1)]
                                         (if- (< (c/get new-member "when") :m2.when.)
                                           [(inc :index.) (inc :index.)]
                                           [(inc :index.) :pos.])))
                                     [0 -1]
                                     :m1.array_2.)
                             :asize. (get :size-position. 0)
                             :position. (get :size-position. 1)]
                         (cond

                           (= :position. -1)                ;;add one start
                           (cond
                                 (= :asize 0)               ;;array was empty so i just add it
                                 [new-member]

                                 (let [:m3. (get :m1.array_2. 0)]
                                   (= :m3.value. (c/get new-member "value")))
                                 :m1.array_2.

                                 :else
                                 (concat [new-member] :m1.array_2.))

                           (= :position. :asize.)           ;;add at the end(i add anyways no value to check)
                           (concat :m1.array_2. [new-member])


                           :else                            ;;add in middle
                           (let [:next-member. (get :m1.array_2. {:index :position.})]
                             (if- (= :next-member.value. (c/get new-member "value"))
                               :m1.array_2.
                               (concat (take 0 :position. :m1.array_2.)
                                       [new-member]
                                       (take :position. :asize. :m1.array_2.))))))})))
           :array_1)}))

(println (clj->json (update- :testdb.testcoll
                      (uq (=- :_id 1)
                          {:array_1
                           (map (fn [:m1.]
                                  (if- (not= :m1.id. 2)
                                    :m1.
                                    (merge :m1.
                                           {:array_2
                                            (let [:size-position.
                                                  (reduce (fn [:index-pos. :m2.]
                                                            (let [:index. (get :index-pos. 0)
                                                                  :pos. (get :index-pos. 1)]
                                                              (if- (< (c/get new-member "when") :m2.when.)
                                                                [(inc :index.) (inc :index.)]
                                                                [(inc :index.) :pos.])))
                                                          [0 -1]
                                                          :m1.array_2.)
                                                  :asize. (get :size-position. 0)
                                                  :position. (get :size-position. 1)]
                                              (cond

                                                (= :position. -1)                ;;add one start
                                                (cond
                                                  (= :asize 0)               ;;array was empty so i just add it
                                                  [new-member]

                                                  (let [:m3. (get :m1.array_2. 0)]
                                                    (= :m3.value. (c/get new-member "value")))
                                                  :m1.array_2.

                                                  :else
                                                  (concat [new-member] :m1.array_2.))

                                                (= :position. :asize.)           ;;add at the end(i add anyways no value to check)
                                                (concat :m1.array_2. [new-member])


                                                :else                            ;;add in middle
                                                (let [:next-member. (get :m1.array_2. {:index :position.})]
                                                  (if- (= :next-member.value. (c/get new-member "value"))
                                                    :m1.array_2.
                                                    (concat (take 0 :position. :m1.array_2.)
                                                            [new-member]
                                                            (take :position. :asize. :m1.array_2.))))))})))
                                :array_1)})
                      {:command true})))


