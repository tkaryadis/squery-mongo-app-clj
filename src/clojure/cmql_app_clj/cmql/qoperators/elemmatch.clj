(ns cmql-app-clj.cmql.qoperators.elemmatch
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


(try (drop-collection :testdb.testcoll) (catch Exception e ""))
(insert :testdb.testcoll {:ar [1 2 3]})

;;------------------------------------simple array match (1 argument call)
(c-print-all (fq :testdb.testcoll
                 (elem-match? :ar (=? 2))
                 [:ar.$]))                ;;optional project, returns array with ONLY the matching member

;;if i have many element-match, $ will be the last match
(c-print-all (fq :testdb.testcoll
                 (elem-match? :ar (=? 2))
                 (elem-match? :ar (=? 3))
                 [:ar.$]))

(try (drop-collection :testdb.testcoll) (catch Exception e ""))
(insert :testdb.testcoll {:ar [{:a 1} {:a 2}]})

;;------------------------------------embeded array match+project (2 arguments call)
(c-print-all (fq :testdb.testcoll
                 (elem-match? :ar (=? :a 2))
                 [:ar.$]))


;;------------------------------------nested array match

(try (drop-collection :testdb.testcoll) (catch Exception e ""))
(insert :testdb.testcoll {:ar [[1 2] [3 4]]})

;;nested array match (nested elem-matches)
(c-print-all (fq :testdb.testcoll
                 (elem-match? :ar (elem-match? (=? 1)))
                 [:ar.$]))      ;;[[1,2]] element match returns array with 1 member, nested elem=>nested $

;;------------------------------------nested array match-embeded

(try (drop-collection :testdb.testcoll) (catch Exception e ""))
(insert :testdb.testcoll {:ar [[{:a 1}] [{:a 2}]]})

;;nested array match embeded (nested elem-matches)
(c-print-all (fq :testdb.testcoll
                 (elem-match? :ar (elem-match? (=? :a 2)))
                 [:ar.$]))      ;;[[1,2]] element match returns array with 1 member, nested elem=>nested $