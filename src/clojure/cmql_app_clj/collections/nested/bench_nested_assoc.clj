(ns cmql-app-clj.collections.nested.bench-nested-assoc
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

;;more on website   collections/nested , perfomance


(update-defaults :client-settings (-> (MongoClientSettings/builder)
                                      (.codecRegistry clj-registry) ;;Remove this if you want to decode in Java Document
                                      (.build)))

(update-defaults :client (MongoClients/create ^MongoClientSettings (defaults :client-settings)))

;;"Elapsed time: 53482.655322 msecs"
;"LEVEL1"
;"Elapsed time: 55796.077397 msecs"
;"Elapsed time: 58385.59405 msecs"
;"LEVEL3"
;"Elapsed time: 76217.563994 msecs"
;"Elapsed time: 124106.075518 msecs"
;"level5"
;"Elapsed time: 99169.750161 msecs"
;"Elapsed time: 175927.213319 msecs"


(try (drop-collection :testdb.testcoll) (catch Exception e ""))

;;Example = update count 1,to count 100

(defn add-docs [n]
  (let [docs (into [] (take n (repeat {
                                       "id" "some_id",
                                       "array1" [
                                                 {
                                                  "_id" "level_1_id"
                                                  "array2" [
                                                            {
                                                             "_id" "level_2_id_0"
                                                             "count" 0
                                                             }
                                                            {
                                                             "_id" "level_2_id_1"
                                                             "count" 1
                                                             }]}]})))]
    (insert :testdb.testcoll docs)))

;;add one million documents like above
(dotimes [_ 50] (add-docs 10000))

(time (.toCollection (q :testdb.testcoll
                        (out :testdb.testcoll1))))

;;------------------------------1 level---------------------------------------------------------------------------------

(prn "LEVEL1")

(time (.toCollection (q :testdb.testcoll
                        (= :id "some_id")
                        (replace-root (merge :ROOT.
                                             {:array1 []}))
                        (out :testdb.testcoll1))))

(time (.toCollection (q :testdb.testcoll
                        (= :id "some_id")
                        (replace-root (assoc-in :ROOT.
                                                ["array1"]
                                                []))
                        (out :testdb.testcoll1)
                        ;{:print true}
                        )))

;;-----------------------------------------3 levels conditions----------------------------------------------------------

(prn "LEVEL3")

(time (.toCollection (q :testdb.testcoll
                        (= :id "some_id")
                        (replace-root (merge :ROOT.
                                             {:array1 (map (fn [:m.]
                                                             (if- (= :m._id. "level_1_id")
                                                                  (merge :m.
                                                                         {:array2 []})
                                                                  :m.))
                                                           :array1)}))
                        (out :testdb.testcoll1))))

(time (.toCollection (q :testdb.testcoll
                        (= :id "some_id")
                        (replace-root (assoc-in :ROOT.
                                                ["array1"
                                                 {:icond (= :v._id. "level_1_id")}
                                                 "array2"]
                                                []))
                        (out :testdb.testcoll1)
                        ;{:print true}
                        )))

;;-----------------------------------------5 levels -------------------------------------------------------------------

(prn "level5")

(time (.toCollection (q :testdb.testcoll
                        (= :id "some_id")
                        (replace-root (merge :ROOT.
                                             {:array1 (map (fn [:m.]
                                                             (if- (= :m._id. "level_1_id")
                                                                  (merge :m.
                                                                         {:array2 (map (fn [:m.]
                                                                                         (if- (= :m._id. "level_2_id_1")
                                                                                              (merge :m.
                                                                                                     {:count 100})
                                                                                              :m.))
                                                                                       :m.array2.)})
                                                                  :m.))
                                                           :array1)}))
                        (out :testdb.testcoll1))))

(time (.toCollection (q :testdb.testcoll
                        (= :id "some_id")
                        (replace-root (assoc-in :ROOT.
                                                ["array1"
                                                 {:icond (= :v._id. "level_1_id")}
                                                 "array2"
                                                 1
                                                 "count"]
                                                100))
                        (out :testdb.testcoll1)
                        ;{:print true}
                        )))