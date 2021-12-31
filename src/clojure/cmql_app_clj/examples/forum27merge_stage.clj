(ns cmql-app-clj.examples.forum27merge-stage
  (:refer-clojure :only [])
  (:use cmql-core.operators.operators
        cmql-core.operators.qoperators
        cmql-core.operators.uoperators
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
  (:require [clojure.core :as c])
  (:import (com.mongodb.client MongoClients MongoCollection MongoClient)
           (com.mongodb MongoClientSettings)))

(update-defaults :client-settings (-> (MongoClientSettings/builder)
                                      (.codecRegistry clj-registry) ;;Remove this if you want to decode in Java Document
                                      (.build)))

(update-defaults :client (MongoClients/create ^MongoClientSettings (defaults :client-settings)))

(defn init-merge-test []
  (do (drop-database :testdb)

      (insert :testdb.coll1
              [{:role_id 1
                :a 2}
               {:role_id 2
                :a 3}
               {:role_id 3
                :a 20}])

      (insert :testdb.coll2
              [{:role_id 1
                :a 2}
               {:role_id 2
                :a 10}
               ])

      (create-index :testdb.coll2 (index [:role_id] {:unique true}))
      ))

(init-merge-test)

(prn "Before")
(c-print-all (q :testdb.coll2))


(.toCollection (q :testdb.coll1
                (unset :_id)
                (merge-s :testdb.coll2
                         (if-match [:role_id]
                                   (let [:p-root. :ROOT.]
                                     (pipeline
                                       (unset :_id)
                                       (replace-root (if- (= :p-root. :ROOT.)
                                                       (merge :ROOT. {:status "updated"})
                                                       :p-root.
                                                       ))))
                                   "insert"))))

#_(println (clj->json (q :testdb.coll1
                       (unset :_id)
                       (merge-s :testdb.coll2
                                (if-match [:role_id]
                                          (let [:p-root. :ROOT.]
                                            (pipeline
                                              (unset :_id)
                                              (replace-root (if- (= :p-root. :ROOT.)
                                                              (merge :ROOT. {:status "updated"})
                                                              :p-root.
                                                              ))))
                                          "insert"))
                       (command))))

(prn)
(prn "after")


(c-print-all (q :testdb.coll2))