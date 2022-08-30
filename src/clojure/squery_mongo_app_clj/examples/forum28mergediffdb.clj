(ns squery-mongo-app-clj.examples.forum28mergediffdb
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


;;https://stackoverflow.com/questions/69333934/how-to-merge-two-collections-keeping-the-document-with-highest-timestamp-in-mong/69334411#69334411

(update-defaults :client-settings (-> (MongoClientSettings/builder)
                                      ; (.codecRegistry clj-registry) ;;Remove this if you want to decode in Java Document
                                      (.build)))

(update-defaults :client (MongoClients/create ^MongoClientSettings (defaults :client-settings)))

(defn init-merge-test []
  (do (drop-database :testdb1)
      (drop-database :testdb2)

      (insert :testdb1.coll
              [{"_id" "K2","value" "VAL3","timest" (date "2021-09-26T09:15:09.942Z")}
               {"_id" "K3","value" "VAL4","timest" (date "2021-09-26T09:15:10.234Z")}])

      (insert :testdb2.coll
              [{"_id" "K1","value" "VAL1","timest" (date "2021-09-26T09:05:09.942Z")}
               {"_id" "K2","value" "VAL2","timest" (date "2021-09-26T09:05:10.234Z")}])

      (create-index :testdb2.coll (index [:timest] {:unique true}))
      ))

(init-merge-test)

(prn "Before")
(c-print-all (q :testdb2.coll))

(.toCollection (q :testdb1.coll
                  (merge-s :testdb2.coll
                           (if-match [:_id]
                                     (let [:p-ROOT. :ROOT.]
                                       (pipeline
                                         (replace-root (if- (> :p-ROOT.timest. :timest)
                                                         :p-ROOT.
                                                         :ROOT.))))
                                     "insert"))))

(prn "After")
(c-print-all (q :testdb2.coll))




(println (clj->json (q :testdb1.coll
                       (merge-s :testdb2.coll
                                (if-match [:_id]
                                          (let [:p-ROOT. :ROOT.]
                                            (pipeline
                                              (replace-root (if- (> :p-ROOT.timest. :timest)
                                                              :p-ROOT.
                                                              :ROOT.))))
                                          "insert"))
                       (command))))

(System/exit 0)


(c-print-all (q :testdb.coll2))