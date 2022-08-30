(ns squery-mongo-app-clj.squery.operators.equalObjects
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

(defn add-docs [n-docs array-size]
  (let [docs (into [] (take n-docs (repeat {:a0 5 :b (into [] (range array-size))})))]
    (insert :testdb.testcoll docs)))

(time (dotimes [_ 100]
        (add-docs 10 10000)))

(time (.toCollection (q :testdb.testcoll
                        {:a1 (let [:a. :a0
                                   :b. :a.
                                   :c. :b.
                                   :d. :c.
                                   :e. :d.
                                   :f. :e.]
                               :f.)}
                        (out :testdb.testcoll1))))

(time (.toCollection (q :testdb.testcoll
                        {:a :a0}
                        {:b :a}
                        {:c :b}
                        {:d :c}
                        {:e :d}
                        {:f :e}
                        {:a1 :f}
                        (unset :a :b :c :d :e :f)
                        (out :testdb.testcoll1))))

(time (.toCollection (q :testdb.testcoll
                        {:a1 (let [:a. :a0
                                   :b. :a.
                                   :c. :b.
                                   :d. :c.
                                   :e. :d.
                                   :f. :e.]
                               :f.)}
                        ;{:print 0}
                        (out :testdb.testcoll1))))

;(try (drop-collection :testdb.testcoll) (catch Exception e ""))

#_(def a {:myobject (ordered-map :a 1
                                 :b 2
                                 :c 3
                                 :d 4
                                 :e 5
                                 :f 6
                                 :g 7
                                 :h 8
                                 :i 9)})

;(prn (type (get a :myobject)) (keys (get a :myobject)))

;(insert :testdb.testcoll a)



#_(c-print-all (q :testdb.testcoll
                  (= :myobject (ordered-map :a 1
                                            :b 2
                                            :c 3
                                            :d 4
                                            :e 5
                                            :f 6
                                            :g 7
                                            :h 8
                                            :i 9))
                  {:print true}))