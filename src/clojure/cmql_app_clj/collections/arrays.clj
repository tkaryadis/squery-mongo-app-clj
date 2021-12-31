(ns cmql-app-clj.collections.arrays
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

(update-defaults :client-settings (-> (MongoClientSettings/builder) (.codecRegistry clj-registry) (.build)))
(update-defaults :client (MongoClients/create ^MongoClientSettings (defaults :client-settings)))
(try (drop-collection :testdb.testcoll) (catch Exception e ""))

;;to make reduce/group fast we need,to create cmql collection to the database that we will use
;;cmql collection is just a collection with 1 empty document
(drop-collection :testdb.cmql)
(insert :testdb.cmql {})

(insert :testdb.testcoll [{:myarray [1 2 2 3]}
                          {:myarray [3 4 4]}])

;;------------------------------MAP/FILTER (ARRAY->ARRAY)---------------------------------------------------------------

;; filter to keep some members
(c-print-all
  (q :testdb.testcoll
     {:filtered (filter (fn [:m.] (> :m. 1)) :myarray)}))

;; map and keep all ($$REMOVE doesnt work to map,it adds nil)
(c-print-all
  (q :testdb.testcoll
     {:maped (map (fn [:m.] (+ :m. 1)) :myarray)}))

(c-print-all
  (q :testdb.testcoll
     {:maped-filtered (filter (fn [:m.]
                                (> :m. 1))
                              (map (fn [:m.] (+ :m. 1)) :myarray))}))


;;-------------------------------Reduce (Array->single value)-----------------------------------------------------------

;; its very fast,as long as the operation (here +_) is O(1)
(c-print-all
  (q :testdb.testcoll
     {:sum (reduce (fn [:a. :n.]
                      (+ :a. :n.))
                    0
                    :myarray)}
     [:!_id :sum]))

;;-------------------------------Reduce (Array->Array)------------------------------------------------------------------

;PROBLEM = conj  is SLOW  (its concat implemented,the only $push we have is the accumulator that cant be used here)
;; reduce to array,using conj (its slow uses concat-)
;; (ok if < 500 members,very slow if 1000+ members,not usable 10000+ members)
(c-print-all
  (q :testdb.testcoll
     {:copy (reduce (fn [:a. :n.]
                       (conj :a. :n.))
                     []
                     :myarray)}
     [:!_id :copy]))



;;-----------------------------Alternative to reduce(Array->Array)------------------------------------------------------
;; (very fast,but pipeline stage not operator => results are added to the root document)
;; (i can later move them to any place with set-in)

;; reduce is done using lookup with pipeline,facet,unwind,group
;; like the above but very fast(no matter how many members),without reduce/conj
;; (requires one dummy collection see docstring of reduce-array)
(c-print-all
  (q :testdb.testcoll
     (reduce-array :myarray  ;any expression that resolves to array,i HAVE to refer to it as :a in the below
                   {:copy       (conj-each :a)
                    :copyFilter (conj-each (if- (> :a 1)
                                                :a
                                                :REMOVE.))
                    :sum        (sum :a)})))

;;group an array (very fast works like the above)
(c-print-all
  (q :testdb.testcoll
     (group-array :myarray  ;any expression,i refer to it as :a in the below
                  {:copy (conj-each :a)
                   :frenquencies (sum 1)
                   :sum (sum :a)}
                  :mygroups   ;;new field with the groups
                  )))

(try (drop-collection :testdb.testcoll) (catch Exception e ""))

(insert :testdb.testcoll [{:myarray [{:name 1 :age 20} {:name 2 :age 20} {:name 3 :age 25}]}
                          {:myarray [{:name 4 :age 30} {:name 5 :age 40} {:name 6 :age 40}]}])

;;group an array on embeded value (4 arguments are needed)
;;(very fast works like the above)
(c-print-all
  (q :testdb.testcoll
     (group-array :myarray
                  :age     ;;the path that i want to group-by,here its :myarray.age
                  {:people (conj-each :a)}
                  :mygroups)))



;;------------------------------NESTED----------------------------------------------------------------------------------
;;reduce-array/group-array puts the result in the root
;;if i want to reduce/group an array that is nested
;;i reduce/group with get-in
;;and then i move the result to that nested location
;;(its very fast like above)

(try (drop-collection :testdb.testcoll) (catch Exception e ""))

(insert :testdb.testcoll  {:mymixedarray [1 {:a "b" :c {:d [1 2 3 2]}}]})

;; get-in,reduce,assoc-in to put it back in place
(c-print-all
  (q :testdb.testcoll
     (reduce-array (get-in :mymixedarray [1 "c" "d"])
                   {:people (conj-each (if- (> :a 1)
                                            :a
                                            :REMOVE.))})
     {:mymixedarray (assoc-in :mymixedarray [1 "c" "d"] :people)}
     (unset :people)))



;;get-in,group,assoc-in to put it back in place (replace the array with the grouped-array)
(c-print-all
  (q :testdb.testcoll
     (group-array (get-in :mymixedarray [1 "c" "d"])
                  {:people (conj-each :a)}
                  :people-groups)
     {:mymixedarray (assoc-in :mymixedarray [1 "c" "d"] :people-groups)}
     (unset :people-groups)))

;;group by field and nested
(try (drop-collection :testdb.testcoll) (catch Exception e ""))

(insert :testdb.testcoll [{:mymixedarray [1 {:a "b" :c {:d [{:age 1} {:age 2} {:age 3} {:age 2}]}}]}
                          {:mymixedarray [1 {:a "b" :c {:d [{:a 1} {:a 2} {:a 3} {:a 2}]}}]}
                          {:mymixedarray [1 {:a "b" :c {:d [1 2 3]}}]}
                          {:mymixedarray [1 2]}])

(c-print-all
  (q :testdb.testcoll
     (group-array (get-in :mymixedarray [1 "c" "d"])
                  :age                                      ;;i only add the extra path
                  {:people (conj-each :a)}
                  :people-groups)
     {:mymixedarray (if- (not-empty? :people-groups)
                         (assoc-in :mymixedarray [1 "c" "d"] :people-groups)
                         :mymixedarray)}
     (unset :people-groups)))