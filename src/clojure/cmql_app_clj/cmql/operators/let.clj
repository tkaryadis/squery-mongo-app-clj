(ns cmql-app-clj.cmql.operators.let
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

(def doc {"_id" 5, "myarray" [0 1 2 3 4 5]})

(insert :testdb.testcoll doc)

;;minimun nested $let are generated here 5,see bellow
(c-print-all (q :testdb.testcoll
                {:a (let [
                          :a. 1                             ;;1
                          :b. 2                             ;;1
                          :c. (+ :a. :b.)                   ;;2
                          :d. (+ :a. :c.)                   ;;3
                          :e. (+ :b. :a. :d.)               ;;4
                          :k. (+ :b. :a. :d. :c. :e.)       ;;5
                          :z. 3                             ;;1
                          :l. :c.                           ;;3
                          ]
                      :k.)}))

;;Generates
;;{"$addFields"
;   {"a"
;    {"$let"
;     {"vars" {"a" 1, "b" 2, "z" 3},
;      "in"
;      {"$let"
;       {"vars" {"c" {"$add" ["$$a" "$$b"]}},
;        "in"
;        {"$let"
;         {"vars" {"d" {"$add" ["$$a" "$$c"]}, "l" "$$c"},
;          "in"
;          {"$let"
;           {"vars" {"e" {"$add" ["$$b" "$$a" "$$d"]}},
;            "in"
;            {"$let"
;             {"vars" {"k" {"$add" ["$$b" "$$a" "$$d" "$$c" "$$e"]}},
;              "in" "$$k"}}}}}}}}}}}}

;;works but not use
(c-print-all (q :testdb.testcoll
                {:a (let [
                          :a. 1
                          :a. 2
                          :b. (+ :a. 0)
                          ]
                      :b.)}))

;;doesnt work
#_(c-print-all (q :testdb.testcoll
                  {:a (let [
                            :a. 1
                            :a. (+ :a. 2)
                            ]
                        :a.)}))


;;--------------------------------let-perfomance------------------------------------------------------------------------

(try (drop-collection :testdb.testcoll) (catch Exception e ""))

(defn add-docs [n]
  (loop [n n
         docs []]
    (if (= n 0)
      (insert :testdb.testcoll docs)
      (recur (dec n) (conj docs {:myarray (into [] (range 1000))})))))

(dotimes [_ 100] (add-docs 100))


(println (clj->json (q :testdb.testcoll
                       {:myarray (map (fn [:n.]
                                        (let [:n1. (+ :n. 1)]
                                          (let [:n2. (+ :n1. 1)]
                                            (let [:n3. (+ :n2. 1)]
                                              (let [:n4. (+ :n3. 1)]
                                                (let [:n5. (+ :n4. 1)]
                                                  (let [:n6. (+ :n5. 1)]
                                                    (let [:n7. (+ :n6. 1)]
                                                      :n7.))))))))
                                      :myarray)}
                       {:command true})
                    {:pretty true}))

(println (clj->json (q :testdb.testcoll
                        {:myarray (map (fn [:n.]
                                         (+ (+ (+ (+ (+ (+ (+ (+ :n. 1) 1) 1) 1) 1) 1) 1) 1))
                                       :myarray)}
                        {:command true}
                        )
                    {:pretty true}))

(time (.toCollection (q :testdb.testcoll
                        {:myarray (map (fn [:n.]
                                         (let [:n1. (+ :n. 1)]
                                           (let [:n2. (+ :n1. 1)]
                                             (let [:n3. (+ :n2. 1)]
                                               (let [:n4. (+ :n3. 1)]
                                                 (let [:n5. (+ :n4. 1)]
                                                   (let [:n6. (+ :n5. 1)]
                                                     (let [:n7. (+ :n6. 1)]
                                                       :n7.))))))))
                                       :myarray)}
                        (out :testdb.testcoll1))))

(time (.toCollection (q :testdb.testcoll
                        {:myarray (map (fn [:n.]
                                         (+ (+ (+ (+ (+ (+ (+ (+ :n. 1) 1) 1) 1) 1) 1) 1) 1))
                                       :myarray)}
                        (out :testdb.testcoll1))))

(time (.toCollection (q :testdb.testcoll
                        {:myarray (map (fn [:n.]
                                         (let [:n1. (+ :n. 1)
                                               :n2. (+ :n. 1)
                                               :n3. (+ :n. 1)
                                               :n4. (+ :n. 1)
                                               :n5. (+ :n. 1)
                                               :n6. (+ :n. 1)
                                               :n7. (+ :n. 1)]
                                           :n7.))
                                       :myarray)}
                        (out :testdb.testcoll1))))

(time (.toCollection (q :testdb.testcoll
                        {:myarray (map (fn [:n.]
                                         (let [:n1. (+ :n. 1)]
                                           (let [:n2. (+ :n1. 1)]
                                             (let [:n3. (+ :n2. 1)]
                                               (let [:n4. (+ :n3. 1)]
                                                 (let [:n5. (+ :n4. 1)]
                                                   (let [:n6. (+ :n5. 1)]
                                                     (let [:n7. (+ :n6. 1)]
                                                       :n7.))))))))
                                       :myarray)}
                        (out :testdb.testcoll1))))


#_(time (dotimes [_ 100]
        (dorun (map (fn [n]
                      (let [n1 (+ n 1)]
                        (let [n2 (+ n1 1)]
                          (let [n3 (+ n2 1)]
                            (let [n4 (+ n3 1)]
                              (let [n5 (+ n4 1)]
                                (let [n6 (+ n5 1)]
                                  (let [n7 (+ n6 1)]
                                    n7))))))))
                    (range 100000)))))

#_(time (dotimes [_ 100] (dorun (map (fn [n]
                                     (+ (+ (+ (+ (+ (+ (+ (+ n 1) 1) 1) 1) 1) 1) 1) 1))
                                   (range 100000)))))