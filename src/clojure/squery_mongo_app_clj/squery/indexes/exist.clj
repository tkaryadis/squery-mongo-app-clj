(ns squery-mongo-app-clj.squery.indexes.exist
  (:refer-clojure :only [])
  (:use squery-mongo-core.operators.operators
        squery-mongo-core.operators.qoperators
        squery-mongo-core.operators.uoperators
        squery-mongo-core.operators.stages
        squery-mongo-core.operators.options
        squery-mongo.driver.cursor
        squery-mongo.driver.document
        squery-mongo.driver.settings
        squery-mongo.driver.transactions
        squery-mongo.driver.utils
        squery-mongo.arguments
        squery-mongo.commands
        squery-mongo.macros
        flatland.ordered.map
        clojure.pprint)
  (:refer-clojure)
  (:require [clojure.core :as c])
  (:import (com.mongodb.client MongoClients MongoCollection MongoClient FindIterable)
           (com.mongodb MongoClientSettings)))

;;a field even if it doesnt exist, it will have a key on index

;;exist true
;;normal index => all index scan(minkey to maxkey) + all connection => i prefer collection scan
;;sparse index very fast

;;exist false
;;normal index => search only those that dont exist (fast if not many)
;;sparse index cant be used => collection scan

(update-defaults :client-settings (-> (MongoClientSettings/builder)
                                      (.codecRegistry clj-registry) ;;Remove this if you want to decode in Java Document
                                      (.build)))

(update-defaults :client (MongoClients/create ^MongoClientSettings (defaults :client-settings)))


(defn add-docs [n]
  (loop [n n
         docs []]
    (if (= n 0)
      (insert :testdb.testcoll docs)
      (recur (dec n) (conj docs (if (< (rand-int 100) 20)
                                  {:a "sfdfsdfsf"
                                   :b 2}
                                  {:a "sfdfsdfsf"}))))))

(do (try (drop-collection :testdb.testcoll) (catch Exception e ""))
    (dotimes [_ 100] (add-docs 1000)))

(try (drop-index :testdb.testcoll [:b]) (catch Exception e ""))

(prn "exists query no index")
(pprint (explain-index (q :testdb.testcoll
                          (exists?? :b)
                          (count-s))))

(try (drop-index :testdb.testcoll [:b]) (catch Exception e ""))
(create-index :testdb.testcoll (index [:b]))


;;range [minkey,maxkey] => all index scan and
(prn "exists query normal index,worse than without index")
(pprint (explain-index (.explain (q :testdb.testcoll
                                    (exists?? :b)
                                    (count-s)))))

(prn "exists query sparse index")
(try (drop-index :testdb.testcoll [:b]) (catch Exception e ""))
(create-index :testdb.testcoll (index [:b] {"sparse" true}))

(pprint (explain-index (q :testdb.testcoll
                          (exists?? :b)
                          (count-s))))


;;---------------------------------not-exists----------------------------------------------------------

(try (drop-index :testdb.testcoll [:b]) (catch Exception e ""))

(prn "not-exists query no index")
(pprint (explain-index (q :testdb.testcoll
                          (not-exists?? :b)
                          (count-s))))

(try (drop-index :testdb.testcoll [:b]) (catch Exception e ""))
(create-index :testdb.testcoll (index [:b]))


;;range [minkey,maxkey] => all index scan and
(prn "not-exists query normal index,fast if big selectivity")
(pprint (explain-index (q :testdb.testcoll
                          (not-exists?? :b)
                          (count-s))))

(prn "not-exists query sparse index, not even used")
(try (drop-index :testdb.testcoll [:b]) (catch Exception e ""))
(create-index :testdb.testcoll (index [:b] {"sparse" true}))

(pprint (explain-index (q :testdb.testcoll
                          (not-exists?? :b)
                          (count-s))))

