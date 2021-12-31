(ns cmql-app-clj.cmql.stages.merge
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
              [{:_id 1 :region "A", :qty 200}
               {:_id 2 :region "B", :qty 300}
               {:_id 3 :region "C", :qty 700}
               {:_id 5 :region "E" :qty 1500}
               {:_id 6 :region "F" :qty 1500}])

      (insert :testdb.coll2
              [
               {:_id 1, :region "A", :qty 400}
               {:_id 2, :region "B", :qty 550}
               {:_id 3, :region "C", :qty 1000}
               {:_id 4, :region "D", :qty 1000}])

      (create-index :testdb.coll2 (index [:region] {:unique true}))
      ))

(init-merge-test)

(q :testdb.coll1
    (merge-s :testdb.coll2
            (if-match [:region]
                      "keepExisting"
                      "insert")))

(println "Test1")
(c-print-all (q :testdb.coll2))


(println "Test2")

(q :testdb.coll1
   ;(unset :_id)     ;;;common but here not needed
   (merge-s :testdb.coll2
            (if-match [:region]
                      (let [:pipelineqty. :qty]
                            (pipeline {:totalqty (+ :pipelineqty. :qty)})) ; pipiline with 1 stage addFields
                      "insert")))

(c-print-all (q :testdb.coll2))