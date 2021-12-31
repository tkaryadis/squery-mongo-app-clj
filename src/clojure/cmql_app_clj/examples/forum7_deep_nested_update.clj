(ns cmql-app-clj.examples.forum7-deep-nested-update
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

;;TODO add assoc-in solution
;;https://developer.mongodb.com/community/forums/t/how-to-update-nested-array-in-complex-schema/8755/2

;;how to push new object in G1>P1>C1->relies array…

(update-defaults :client-settings (-> (MongoClientSettings/builder)
                                      (.codecRegistry clj-registry) ;;Remove this if you want to decode in Java Document
                                      (.build)))

(update-defaults :client (MongoClients/create ^MongoClientSettings (defaults :client-settings)))

(try (drop-collection :testdb.testcoll) (catch Exception e ""))

(def docs [{"_id" "G1",
            "name" "My Group",
            "posts"
            [{"_id" "P1",
              "title" "Post 1",
              "comments"
              [{"_id" "C1",
                "name" "Comment 1",
                "replies"
                [{"_id" "R1", "content" "Reply 1"}
                 {"_id" "R2", "content" "Reply 2"}]}
               {"_id" "C2",
                "name" "Comment 2",
                "replies"
                [{"_id" "R3", "content" "Reply 3"}
                 {"_id" "R4", "content" "Reply 4"}]}]}]}
           {"_id" "G2",
            "name" "My Group",
            "posts"
            [{"_id" "P2",
              "title" "Post 2",
              "comments"
              [{"_id" "C3",
                "name" "Comment 3",
                "replies"
                [{"_id" "R5", "content" "Reply 5"}
                 {"_id" "R6", "content" "Reply 6"}]}
               {"_id" "C4",
                "name" "Comment 4",
                "replies"
                [{"_id" "R7", "content" "Reply 7"}
                 {"_id" "R8", "content" "Reply 8"}]}]}]}])

(insert :testdb.testcoll docs)

;;handmade solution
(update-
  :testdb.testcoll
  (uq (replace-root
        (if- (= :ROOT._id. "G1")
          (merge
            :ROOT.
            {:posts
             (let [:posts. :ROOT.posts.]
               (map (fn [:post.]
                      (if- (= :post._id. "P1")
                        (merge :post.
                               {:comments
                                (map (fn [:comment.]
                                       (if- (= :comment._id. "C1")
                                         (merge :comment.
                                                {:replies
                                                 (conj :comment.replies. {"_id"         "R3",
                                                                          "content"     "Reply 3"
                                                                          "randomField" "EDIT THIS DOC"})})
                                         :comment.))
                                     :post.comments.)})
                        :post.))
                    :posts.))})
          :ROOT.))))

