(ns cmql-app-clj.collections.nested.update
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


(def nested-doc {
        :myarray  [1 [2 [3 4 5] 6] 7 8]
        :myobject {:a "b" :c {:d "e" :f {:g "h"}}}
        :mymixedobject {:a "b" :c {:d [1 2 3]}}
        :mymixedarray [1 {:a "b" :c {:d [1 2 3]}}]
        })

(insert :testdb.testcoll nested-doc)

(def c
  (time (q :testdb.testcoll
           {
            :v1 (= (assoc :myarray 1 10) [1 10 7 8])
            :v2 (= (assoc-in :myarray [1] 10) [1 10 7 8])
            :v3 (= (assoc-in :myarray [1 1] 10) [1 [2 10 6] 7 8])
            :v4 (= (assoc-in :myarray [1 1 1] 10) [1 [2 [3 10 5] 6] 7 8])
            :v5 (= (assoc-in :myarray [{:icond (= :v. 1)}] 200) [200 [2 [3 4 5] 6] 7 8])        ; find the member=1 and set it to 200
            :v6 (= (assoc-in :myarray [{:icond (array? :v.)} 0] 200) [1 [200 [3 4 5] 6] 7 8])  ; find the array member and set at index 0 the value 200
            :v7 (= (assoc-in :myarray [{:icond (array? :v.)} 1 {:icond (= :v. 4)}] 200) [1 [2 [3 200 5] 6] 7 8] )  ;get in array,index 1,member_value=4 set it to 200

            ;:myobject {:a "b" :c {:d "e" :f {:g "h"}}}

            ;;equality in mongo takes acount the order,and its not used
            :v8  (assoc :myobject "c" "new")
            :v9  (assoc-in :myobject ["c" "f" "g"] "new")
            :v10 (assoc-in :myobject [{:kcond (= :k. "c")}] "new")
            :v11 (assoc-in :myobject [{:kcond (object? :v.)} "f" "g"] "new")

            ;;:mymixedarray [1 {:a "b" :c {:d [1 2 3]}}]

            :v12 (= (assoc-in :mymixedarray [1 "c" "d" 0] 10) [1 {:a "b" :c {:d [10 2 3]}}])


            ;:mymixedobject {:a "b" :c {:d [1 2 3]}}

            :v13 (assoc-in :mymixedobject ["c" "d" 1] 10)

            }
           [:!_id :v1 :v2  :v3 :v4 :v5 :v6 :v7 :v8 :v9 :v10 :v11 :v111 :v12 :v13]
           ;(command)
           )))

#_(let [m (:v11 (first (c-take-all c)))]
  (prn (= m {:c {:d "e", :f {:g "new"}}, :a "b"})))

(c-print-all c)
;(println (count (str c)))