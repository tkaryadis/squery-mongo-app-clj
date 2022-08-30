(ns squery-mongo-app-clj.mongodb-forum.forum3-change-structure
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
  (:import (com.mongodb.client MongoClients MongoCollection MongoDatabase MongoClient)
           (com.mongodb MongoClientSettings)))


;;https://developer.mongodb.com/community/forums/t/want-to-change-the-document-structure/113655

(update-defaults :client-settings (-> (MongoClientSettings/builder)
                                      (.codecRegistry clj-registry) ;;Remove this if you want to decode in Java Document
                                      (.build)))

(update-defaults :client (MongoClients/create ^MongoClientSettings (defaults :client-settings)))

(try (drop-collection :testdb.testcoll) (catch Exception e ""))

(def doc {
          "_id" "60546e16000cc037474fad2f"
          "firstVisitDate" "2021-03-19T09:25:42.892Z"
          "lastVisitDate" "2021-03-19T12:57:58.885Z"
          "platform"  "gooogle"
          "visits" 5
          })


(insert :testdb.testcoll doc)

(c-print-all (update- :testdb.testcoll
               (uq (replace-root (let [:doc2-array. [[:platform {:visits         :visits
                                                                 :firstVisitDate :firstVisitDate
                                                                 :lastVisitDate  :lastVisitDate}]]]
                                   (let [:doc2. (into {} :doc2-array.)]
                                     {:_id      :_id
                                      :platform :doc2.}))))
               ;{:print true}
               ))



