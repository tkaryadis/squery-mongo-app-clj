(ns squery-mongo-app-clj.examples.ex1-map-index
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

(def doc {:myarray [1 2 3 4 5]})

(insert :testdb.testcoll doc)

;;process each member of an array,while knowing its index

;;ziparray + map solution
(c-print-all (q :testdb.testcoll
                {:myarray (map (fn [:m-index.]
                                 (let [:m. (get :m-index. 0)
                                       :index. (get :m-index. 1)]
                                   (if- (= :index. 3)
                                        (+ :m. 10)
                                        :m.)))
                               (ziparray :myarray (range (count :myarray))))}
                ;{:print true}
                ))

;;reduce solution(more general)
(c-print-all (q :testdb.testcoll
                {:myarray (get (reduce (fn [:arrray-index. :m.]
                                         (let [:array. (get :arrray-index. 0)
                                               :index. (get :arrray-index. 1)]
                                           (if- (= :index. 3)
                                             [(conj :array. (+ :m. 10)) (inc :index.)]
                                             [(conj :array. :m.) (inc :index.)])))
                                       [[] 0]
                                       :myarray)
                               0)}
                ;{:print true}
                ))