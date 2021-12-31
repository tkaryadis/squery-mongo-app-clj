(ns cmql-app-clj.examples.forum24lookupArrayIndex
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
  (:require [clojure.core :as c]
            [clojure.data.json :as tojson])
  (:import (com.mongodb.client MongoClients MongoCollection MongoClient AggregateIterable)
           (com.mongodb MongoClientSettings)
           (org.bson.types ObjectId)
           (java.util Date Calendar)
           (com.mongodb.client.model Indexes)))


;;https://stackoverflow.com/questions/68871092/check-if-a-documents-value-is-in-an-array-mongodb

(update-defaults :client-settings (-> (MongoClientSettings/builder)
                                      (.codecRegistry clj-registry)
                                      (.build)))

(update-defaults :client (MongoClients/create ^MongoClientSettings (defaults :client-settings)))


(try (drop-collection :testdb.collections) (catch Exception e ""))
(try (drop-collection :testdb.artworks) (catch Exception e ""))
(try (drop-collection :testdb.dummy) (catch Exception e ""))

(def collections [{"_id" "612fa9f6b2cc520a84e83dde",
                   "name" "Name",
                   "author" "5fe8fe53ff8d9dc25c9e3277",
                   "date" 1
                   "itemsCount" 3}])

(def artworks [{"_id" "612fa9541121d06014e7d9bc",
            "collections" ["612fa9481121d06014e7d9b5" "612fa9f6b2cc520a84e83dde"],
            "author" "5fe8fe53ff8d9dc25c9e3277",
            "text" "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Etiam vestibulum dolor id justo condimentum ",
            "type" "Text"}
           {"_id" "60eec90e787a0b320c514446",
            "author" "5fe8fe53ff8d9dc25c9e3277",
            "type" "Image",
            "asset" "60eec90e787a0b320c514441",
            "collections" ["612fa9f6b2cc520a84e83dde"]}
           {"_id"  "612fa9f2b2cc520a84e83db1",
            "collections" ["612fa9f6b2cc520a84e83dde"],
            "author" "5fe8fe53ff8d9dc25c9e3277",
            "text" "Lorem ipsum dolor sit amet",
            "type" "Text"}
           {"_id" "612549d4778270001862472f",
            "author" "5fe8fe53ff8d9dc25c9e3277",
            "type" "Video",
            "asset" "612549d4778270001862472a"}])



(insert :testdb.collections collections)
(insert :testdb.artworks artworks)
(insert :testdb.dummy [{}])


(c-print-all (q :testdb.collections
                {"$match" {"author" "5fe8fe53ff8d9dc25c9e3277"}}
                {"$lookup"
                 {"from" "artworks",
                  "as" "previews",
                  "let" {"collection" "$_id"},
                  "pipeline"
                         [{"$match"
                           {"collections" {"$exists" true},
                            "$expr" {"$in" ["$$collection" "$collections"]}}}
                          {"$sort" {"date" -1}}
                          {"$limit" 4}
                          {"$addFields" {"id" "$_id"}}]}}))

(prn "-----------------------------------------------------------------------------------------------------------")


(c-print-all (q :testdb.collections
                (=- :author "5fe8fe53ff8d9dc25c9e3277")
                (lookup :_id :artworks.collections :previews)
                (lookup-p :dummy
                            [:previews. :previews]
                            [{:previews :previews.}
                             (unwind :previews)
                             (replace-root :previews)
                             (sort :!date)
                             (limit 4)
                             {:id :_id}
                             (unset :_id)]
                            :previews)))



