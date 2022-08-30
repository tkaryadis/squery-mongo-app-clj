(ns squery-mongo-app-clj.squery.indexes.expr
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

;;ONLY those $eq, $lt, $lte, $gt, $gte aggregate can use index
;; $or and $and works for aggregation also
;;Limitations
;; Multikey indexes are not used.
;  Indexes are not used for comparisons where the operand is an array
;   or the operand type is undefined.
;  Indexes are not used for comparisons with more than one field path operand.

(update-defaults :client-settings (-> (MongoClientSettings/builder)
                                      (.codecRegistry clj-registry) ;;Remove this if you want to decode in Java Document
                                      (.build)))

(update-defaults :client (MongoClients/create ^MongoClientSettings (defaults :client-settings)))


(try (drop-collection :testdb.testcoll) (catch Exception e ""))

(def counter (atom 0))
(defn add-docs [n]
  (loop [n n
         docs []]
    (if (= n 0)
      (insert :testdb.testcoll docs)
      (recur (dec n) (conj docs {:a (swap! counter inc)
                                 :b (rand-int 1000)})))))

(dotimes [_ 100] (add-docs 1000))

;;no index
(try (drop-index :testdb.testcoll [:a]) (catch Exception e ""))
(pprint (explain-index (.explain (q :testdb.testcoll
                                    (< :a 20)
                                    (count-s)))))

;;index
(try (drop-index :testdb.testcoll [:a]) (catch Exception e ""))
(try (drop-index :testdb.testcoll [:b]) (catch Exception e ""))
(create-index :testdb.testcoll (index [:a]))

(pprint (explain-index (.explain (q :testdb.testcoll
                                    (< :a 20)
                                    (count-s)))))

;;index
(try (drop-index :testdb.testcoll [:a]) (catch Exception e ""))
(try (drop-index :testdb.testcoll [:b]) (catch Exception e ""))
(create-index :testdb.testcoll (index [:a]))

(pprint (explain-index (.explain (q :testdb.testcoll
                                    (not= :a 20)
                                    (count-s)))))

;;Testing $or and $and
(try (drop-index :testdb.testcoll [:a]) (catch Exception e ""))
(try (drop-index :testdb.testcoll [:b]) (catch Exception e ""))
(create-index :testdb.testcoll (index [:a]))
(create-index :testdb.testcoll (index [:b]))

(prn "test or aggregate")
(pprint (explain-index (.explain (q :testdb.testcoll
                                    (and (= :a 20) (= :b 20))
                                    (count-s)))))

;;Testing $in aggregate (doesnt work)

(try (drop-collection :testdb.testcoll) (catch Exception e ""))
(def counter (atom 0))
(defn add-docs [n]
  (loop [n n
         docs []]
    (if (= n 0)
      (insert :testdb.testcoll docs)
      (recur (dec n) (conj docs {:a (take 10 (repeat (rand-int 10000)))})))))

(dotimes [_ 100] (add-docs 1000))

(try (drop-index :testdb.testcoll [:a]) (catch Exception e ""))
(try (drop-index :testdb.testcoll [:b]) (catch Exception e ""))
(create-index :testdb.testcoll (index [:a]))

(pprint (explain-index (.explain (q :testdb.testcoll
                                    (contains? :a 20)
                                    (count-s)))))