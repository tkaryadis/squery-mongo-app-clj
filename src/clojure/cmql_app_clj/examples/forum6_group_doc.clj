(ns cmql-app-clj.examples.forum6-group-doc
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
  (:import (com.mongodb.client MongoClients MongoCollection MongoDatabase MongoClient)
           (com.mongodb MongoClientSettings)))

;;https://developer.mongodb.com/community/forums/t/how-can-i-find-all-docs-where-a-user-has-only-a-single-doc-aggregation-problem/8339

(update-defaults :client-settings (-> (MongoClientSettings/builder)
                                      (.codecRegistry clj-registry) ;;Remove this if you want to decode in Java Document
                                      (.build)))

(update-defaults :client (MongoClients/create ^MongoClientSettings (defaults :client-settings)))

(try (drop-collection :testdb.testcoll) (catch Exception e ""))


(def docs [{ "kit1" "AA", "kit2" "ABV", "chr"  "1"  }
           { "kit1" "XX", "kit2" "CC", "chr" "5" }
           { "kit1" "BB", "kit2" "HR" , "chr"  "8" }
           {  "kit1" "BB" , "kit2" "HR", "chr" "X"  }])

(insert :testdb.testcoll docs)

(def kits ["AA" , "BB", "CC"])

(c-print-all (q :testdb.testcoll
                (or (contains? kits :kit1) (contains? kits :kit2))
                (group {:_id {:kit1 :kit1
                              :kit2 :kit2}}
                       {:doc (conj-each {
                                         :kit1 :kit1
                                         :kit2 :kit2
                                         :chr  :chr
                                         })})
                (= (count :doc)  1)
                (unwind-replace-root :doc)))

