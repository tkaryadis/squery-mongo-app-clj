(ns squery-mongo-app-clj.examples.forum19arraysfilterpush
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
  (:import (com.mongodb.client MongoClients)
           (com.mongodb MongoClientSettings)))

(update-defaults :client-settings (-> (MongoClientSettings/builder)
                                      (.codecRegistry clj-registry) ;;Remove this if you want to decode in Java Document
                                      (.build)))

(update-defaults :client (MongoClients/create ^MongoClientSettings (defaults :client-settings)))

;;https://stackoverflow.com/questions/68674282/pull-an-item-from-all-arrays-mongodb/68674400#68674400

(def data {:votes {
                   "1"  [ "619284841187246090", "662697094104219678" ],
                   "2" [ "619284841187246090", "662697094104219678" ],
                   "3" [ "662697094104219678", "619284841187246090" ]
                   }})


(try (drop-collection :testdb.testcoll) (catch Exception e ""))

(insert :testdb.testcoll data)

(println (clj->json (q :testdb.testcoll
                       {:votes (into {} (map (fn [:vote.]
                                               (if- (= (get :vote. 0) "2")
                                                 [(get :vote. 0) (conj-distinct (get :vote. 1) "619284841187246090")]
                                                 [(get :vote. 0) (filter (fn [:v.]
                                                                           (not (= :v. "619284841187246090")))
                                                                         (get :vote. 1))]))
                                             (into [] :votes)))}
                       {:command true})
                    {:pretty true}))

