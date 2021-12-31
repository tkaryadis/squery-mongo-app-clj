(ns cmql-app-clj.examples.forum15pullall
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
  (:import (com.mongodb.client MongoClients)
           (com.mongodb MongoClientSettings)))

;;https://www.mongodb.com/community/forums/t/pull-or-pullall-to-delete-array-of-emails/117503

(update-defaults :client-settings (-> (MongoClientSettings/builder)
                                      (.codecRegistry clj-registry) ;;Remove this if you want to decode in Java Document
                                      (.build)))

(update-defaults :client (MongoClients/create ^MongoClientSettings (defaults :client-settings)))

(try (drop-collection :testdb.testcoll) (catch Exception e ""))

(def doc {"userid" 1
          "fullname"  "john-don"
          "contacts" [ {"email" "gates@microsoft.com"}
                       {"email" "me@gmail.com"}]})

(insert :testdb.testcoll doc)


(def delete-emails-docs [{"email" "tim@apple.com"} {"email" "gates@microsoft.com"} {"email" "elon@tesla.com"}])

(def coll (.getCollection (.getDatabase (defaults :client) "testdb") "testcoll"))

#_(.findOneAndUpdate coll
                   (f (=- :userid 1))
                   (d {"$pullAll" {"contacts" delete-emails-docs}}))

;(c-print-all (q :testdb.testcoll))

(def delete-emails ["tim@apple.com", "gates@microsoft.com","elon@tesla.com"])

(.findOneAndUpdate coll
                   (f (=- :userid 1))
                   (p {:contacts (filter (fn [:contact.]
                                           (not (contains? delete-emails :contact.email.)))
                                         :contacts)}))
(c-print-all (q :testdb.testcoll))
