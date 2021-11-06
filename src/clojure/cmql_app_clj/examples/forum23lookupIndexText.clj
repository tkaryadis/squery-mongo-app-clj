(ns cmql-app-clj.examples.forum23lookupIndexText
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
                                      ;(.codecRegistry clj-registry)
                                      (.build)))

(update-defaults :client (MongoClients/create ^MongoClientSettings (defaults :client-settings)))


(try (drop-collection :testdb.testcoll) (catch Exception e ""))
(try (drop-collection :testdb.testcoll1) (catch Exception e ""))


(try (drop-collection :testdb.testcoll2) (catch Exception e ""))
(try (drop-collection :testdb.testcoll3) (catch Exception e ""))
(try (drop-collection :testdb.testcoll4) (catch Exception e ""))
(try (drop-collection :testdb.testcoll5) (catch Exception e ""))

(defn add-docs [ndocs start]
  (loop [n start
         docs []]
    (if (= n (+ ndocs start))
      (insert :testdb.testcoll docs)
      (recur (inc n) (conj docs {:afield  "",
                                 :content (if (< (rand) 0.5)
                                            (str "ABCDE" n)
                                            (str "tewww" n))})))))


(add-docs 100000 0)
(add-docs 100000 100000)

(def list-docs (mapv #(str "abcde" %)
                     (range 40000)))

(def list-docs-str (clojure.string/join " " list-docs))

(insert :testdb.testcoll1 (mapv (fn [l]
                                  {:list-content l})
                                list-docs))

(def coll (.getCollection (.getDatabase (defaults :client) "testdb") "testcoll"))
(.createIndex coll (Indexes/text "content"))

;(def coll1 (.getCollection (.getDatabase (defaults :client) "testdb") "testcoll1"))
;(.createIndex coll1 (Indexes/text "list-content"))

(time (.toCollection (q :testdb.testcoll
                        { "$match"  { "$text"  { "$search"  list-docs-str} } }
                        (out :testdb.testcoll2))))

(prn "------------------")

#_(time (.toCollection (q :testdb.testcoll
                        (contains? list-docs (lower-case :content))
                        (out :testdb.testcoll3))))

(System/exit 0)

#_(time (.toCollection (q :testdb.testcoll
                        {:content (lower-case :content)}
                        (lookup :content :testcoll1.list-content :joined)
                        (not-empty? :joined)
                        (unset :joined)
                        (out :testdb.testcoll3))))

#_(create-index :testdb.testcoll1 (index [:list-content]))

#_(time (.toCollection (q :testdb.testcoll
                        {:content (lower-case :content)}
                        (lookup :content :testcoll1.list-content :joined)
                        (not-empty? :joined)
                        (unset :joined)
                        (out :testdb.testcoll4))))

#_(create-index :testdb.testcoll (index [:content]
                                      {"collation" {
                                                    "locale"   "en",
                                                    "strength" 2
                                                    }}))

;(create-index :testdb.testcoll (index [{:content "text"}]))

;;dbCollection.createIndex(Indexes.text("some_field3"));

#_(def coll (.getCollection (.getDatabase (defaults :client) "testdb") "testcoll"))

#_(.createIndex coll (Indexes/text "content"))

#_(.createIndex ^MongoCollection coll (Indexes/ascending (into-array ["content"])))

(time (.toCollection (q :testdb.testcoll
                        (lookup :content :testcoll1.list-content :joined)
                        (not-empty? :joined)
                        (out :testdb.testcoll5))))

#_(clojure.pprint/pprint (json->clj (.toJson (.explain ^AggregateIterable (q :testdb.testcoll
                                                                           (= :content "hello")
                                                                           #_(lookup :content :testcoll1.list-content :joined))))))

