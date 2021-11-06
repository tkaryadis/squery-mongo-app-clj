(ns cmql-app-clj.examples.forum11UpdateOrPushArray
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
  (:import (com.mongodb.client MongoClients MongoCollection MongoDatabase MongoClient)
           (com.mongodb MongoClientSettings)))

;;https://www.mongodb.com/community/forums/t/perform-multiple-updates-upserts-on-embedded-documents/115608/4

(update-defaults :client-settings (-> (MongoClientSettings/builder)
                                      (.codecRegistry clj-registry) ;;Remove this if you want to decode in Java Document
                                      (.build)))

(update-defaults :client (MongoClients/create ^MongoClientSettings (defaults :client-settings)))

(try (drop-collection :testdb.testcoll) (catch Exception e ""))

(def data {"_id" "1",
           "lastName" "Cary",
           "firstName" "paul",
           "dob" "2012-12-12",
           "friends" [{"_id" "friend_1", "lastName" "Eric"} {"_id" "friend_3", "lastName" "Charan"}]})

(println (clj->json {"_id" "1",
                     "lastName" "Cary",
                     "firstName" "paul",
                     "dob" "2012-12-12",
                     "friends" [{"_id" "friend_1", "lastName" "Eric"} {"_id" "friend_3", "lastName" "Charan"}]}))

(insert :testdb.testcoll data)

#_(update- :testdb.testcoll
         (uq {:_id "10"}
             {:friends (if- (exists? :friends)
                         (concat (map (fn [:friend.]
                                        (if- (= :friend._id. "friend_1")
                                          (merge :friend. {:firstName "Karan"})
                                          :friend.))
                                      :friends)
                                 [{"_id" "friend_2"
                                   "firstName" "Chasay"}])
                         [])}
             (unset :firstName :dob)
             {:upsert true})
        {:print true})


(def new-friend {"_id" "friend_10", "lastName" "Don"})

(update- :testdb.testcoll
  (uq (= :_id "1")
      {:friends (let [:isNew. (empty? (filter (fn [:friend.]
                                                (= :friend._id. (c/get new-friend "_id")))
                                              :friends))]
                  (if- :isNew.
                    (concat :friends [new-friend])
                    (map (fn [:friend.]
                           (if- (= :friend._id. (c/get new-friend "_id"))
                             (merge :friend. new-friend)
                             :friend.))
                         :friends)))})
  {:print "js"})