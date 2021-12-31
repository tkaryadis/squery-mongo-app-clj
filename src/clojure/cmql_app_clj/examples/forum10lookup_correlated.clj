(ns cmql-app-clj.examples.forum10lookup-correlated
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

;;https://www.mongodb.com/community/forums/t/select-with-sub-query/115744/4

(update-defaults :client-settings (-> (MongoClientSettings/builder)
                                      (.codecRegistry clj-registry) ;;Remove this if you want to decode in Java Document
                                      (.build)))

(update-defaults :client (MongoClients/create ^MongoClientSettings (defaults :client-settings)))

(drop-collection :testdb.users)
(drop-collection :testdb.posts)
(drop-collection :testdb.likes)

(def users [{:_id 1 :name "peter"}
            {:_id 2 :name "john"}
            {:_id 3 :name "bob"}])

(def posts [{:_id 1 :content "post1"}
            {:_id 2 :content "post2"}
            {:_id 3 :content "post3"}
            {:_id 4 :content "post4"}
            {:_id 5 :content "post5"}])

(def likes [{:user_id 1 :post_id 1}
            {:user_id 1 :post_id 2}
            {:user_id 1 :post_id 3}
            {:user_id 2 :post_id 1}
            {:user_id 2 :post_id 4}])


(insert :testdb.users users)
(insert :testdb.posts posts)
(insert :testdb.likes likes)

(c-print-all (q :testdb.posts
                (lookup-p :likes
                          [:posts_id. :_id]
                          [(= :user_id 1)
                           (= :post_id :posts_id.)
                           [:!_id :user_id]]
                          :liked)
                (unwind :liked {:preserveNullAndEmptyArrays true})
                [:!_id
                 {:user_id (if- (exists? :liked.user_id)
                             :liked.user_id
                             nil)}
                 :content]
                {:print true}))

(println (clj->json {"aggregate" "posts",
                     "print" true,
                     "pipeline"
                                 [{"$lookup"
                                   {"from" "likes",
                                    "let" {"posts_id" "$_id"},
                                    "pipeline"
                                           [{"$match"
                                             {"$expr"
                                              {"$and"
                                               [{"$eq" ["$user_id" 1]} {"$eq" ["$post_id" "$$posts_id"]}]}}}
                                            {"$project" {"_id" 0, "user_id" 1}}],
                                    "as" "liked"}}
                                  {"$unwind" {"path" "$liked", "preserveNullAndEmptyArrays" true}}
                                  {"$project"
                                   {"_id" 0,
                                    "user_id"
                                          {"$cond"
                                           [{"$ne" [{"$type" "$liked.user_id"} "missing"]}
                                            "$liked.user_id"
                                            nil]},
                                    "content" 1}}],
                     "cursor" {},
                     "maxTimeMS" 1200000}))

(c-print-all (q :testdb.posts
                (lookup-p :likes
                          [:posts_id. :_id]
                          [(match {:user_id 1})
                           (= :post_id :posts_id.)
                           [:!_id :user_id]]
                          :liked)
                (unwind :liked {:preserveNullAndEmptyArrays true})
                [:!_id
                 {:user_id (if- (exists? :liked.user_id)
                             :liked.user_id
                             nil)}
                 :content]
                {:print true}))

#_(c-print-all (q :testdb.likes
                (match {:user_id 1})
                (lookup :post_id :posts._id :liked)
                (unwind :liked)
                (replace-root (merge :ROOT. :liked))
                (unset :_id :post_id :liked)
                {:print true}))

#_(println (clj->json {"aggregate" "likes",
                     "pipeline"
                                 [{"$match" {"user_id" 1}}
                                  {"$lookup"
                                   {"from" "posts",
                                    "localField" "post_id",
                                    "foreignField" "_id",
                                    "as" "liked"}}
                                  {"$unwind" {"path" "$liked"}}
                                  {"$replaceRoot" {"newRoot" {"$mergeObjects" ["$$ROOT" "$liked"]}}}
                                  {"$unset" ["_id" "post_id" "liked"]}],
                     "cursor" {},
                     "maxTimeMS" 1200000}))


#_(println (clj->json {"aggregate" "likes",
                     "print" true,
                     "pipeline"
                                 [{"$match"
                                   {"$expr" {"$and" [{"$eq" ["$post_id" 1]} {"$eq" ["$user_id" 2]}]}}}
                                  {"$lookup"
                                   {"from" "posts",
                                    "localField" "post_id",
                                    "foreignField" "_id",
                                    "as" "joined__"}}
                                  {"$unwind" {"path" "$joined__"}}
                                  {"$replaceRoot" {"newRoot" {"$mergeObjects" ["$joined__" "$$ROOT"]}}}
                                  {"$project" {"joined__" 0}}
                                  {"$lookup"
                                   {"from" "users",
                                    "localField" "user_id",
                                    "foreignField" "_id",
                                    "as" "joined__"}}
                                  {"$unwind" {"path" "$joined__"}}
                                  {"$replaceRoot" {"newRoot" {"$mergeObjects" ["$joined__" "$$ROOT"]}}}
                                  {"$project" {"joined__" 0}}
                                  {"$unset" ["_id"]}],
                     "cursor" {},
                     "maxTimeMS" 1200000}))