(ns cmql-app-clj.examples.ex1-map-index
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