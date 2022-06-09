(ns cmql-app-clj.cmql.uoperators.pull
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

(insert :testdb.testcoll [{"car" "Honda",
                           "color" [nil nil nil nil nil nil nil nil nil "red" "orange"]}])


(pprint (update- :testdb.testcoll
              (uq (remove! :color (=? nil)))))

(c-print-all (q :testdb.testcoll))


(try (drop-collection :testdb.testcoll) (catch Exception e ""))

(insert :testdb.testcoll [{"person" [{"name" "takis"},{"name" "john"}]}])

(update- :testdb.testcoll
         (uq (remove! :person (=? :name "takis"))))

(c-print-all (q :testdb.testcoll))

(try (drop-collection :testdb.testcoll) (catch Exception e ""))

(insert :testdb.testcoll [{"person" [{"women" [{"name" "helen"} {"name" "julia"}]}]}])


(c-print-all (q :testdb.testcoll))


;;db.survey.updateMany(
;  { },
;  {
;     $pull:
;        {
;           results:
;              {
;                 answers: { $elemMatch: { q: 2, a: { $gte: 8 } } }
;              }
;        }
;  }
;)

(update- :testdb.testcoll
         (uq (remove! :person (elem-match? :women (=? :name "helen")))))

(pprint (update- :testdb.testcoll
                 (uq (remove! :person (elem-match? :women (=? :name "helen"))))
                 (command)))

(c-print-all (q :testdb.testcoll))




