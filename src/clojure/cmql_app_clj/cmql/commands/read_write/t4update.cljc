(ns cmql-app-clj.cmql.commands.read_write.t4update
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
(try (drop-collection :testdb.types) (catch Exception e ""))
(try (drop-collection :testdb.grades) (catch Exception e ""))


;;--------------------------insert--------------------------------------------------------
(def docs [{
            "_id"  1
            "name"  "MongoDB"
            "type"  "database"
            "versions" [ "v3.2" "v3.0" "v2.6" ]
            "info" { "x"  203 "y"  102 }
            }
           {
            "_id"  2
            "name"  "MongoDB"
            "type"  "database"
            "versions" [ "v3.2" "v3.0" "v2.6" ]
            "info" { "x"  203 "y"  102 }
            }
           {
            "_id"  3
            "name"  "MongoDB"
            "type"  "databases"
            "versions" [ "v3.2" "v3.0" "v2.6" ]
            "info" { "x"  203 "y"  102 }
            }])

(insert :testdb.testcoll docs)

;;--------------------------updateOne--------------------------------------------------------

;;print all document of the collection,including the one we inserted
(println "----------After Insert------------")
(c-print-all (q :testdb.testcoll))

(update- :testdb.testcoll
         (uq (= :_id 2)
             {:name (str :name "-hello-id2")}
             [:name :versions]))

(println "----------After Update------------")
(c-print-all (q :testdb.testcoll))


;;--------------------------updateOneOptions--------------------------------------------------------

(update- :testdb.testcoll
         (uq (= :_id 1)
             {:name (str :name "-hello-id1")})
         {:bypassDocumentValidation  true})

;;print all document of the collection,including the one we inserted
#_(println "----------After UpdateOneOptions------------")
#_(c-print-all db-coll (q db-coll))

;;----------------------update and operators---------------------------------------------------------


(insert :testdb.mytypes
        [{ :_id 1
          :myint 10
          :mydouble 0.50
          :mybool true
          :mystring "gggddeee"
          :myarray [1 2 5 10]
          :myobject1 {:a 2}}
         { :_id 2
          :myint 10
          :mydouble 0.50
          :mybool true
          :mystring "gggddeee"
          :myarray [1 2 5 10]
          :myobject1 {:a 2}}])

(println "----------After Insert------------")
(c-print-all (q :testdb.mytypes))


;;------------------------------Example--------------------------------------------
;;https://docs.mongodb.com/manual/reference/operator/update/positional-filtered/

(insert :testdb.grades
        [{ "_id"  1, "grades"  [ 95, 92, 90 ] }
         { "_id"  2, "grades"  [ 98, 100, 102 ] }
         { "_id"  3, "grades"  [ 95, 110, 100 ] }])

;;if grade>=100 set value 100
;;db.students.update(
;   { },
;   { $set: { "grades.$[element]" : 100 } },
;   { multi: true,
;     arrayFilters: [ { "element": { $gte: 100 } } ]
;   }
;)

(println "----------After Insert!------------")
(c-print-all (q :testdb.grades))


;;update with pipeline is the default way
(c-print-all (update- :testdb.grades
                      (uq (= :_id 2)
                          {:grades (map (fn [:member.]
                                              (if- (>= :member. 100) 100 :member.))
                                         :grades)})))

(println "----------After Update------------")
(c-print-all (q :testdb.grades))

(drop-collection :testdb.grades)

(insert :testdb.grades
        [{ "_id"  1, "grades"  [ 95, 92, 90 ] }
         { "_id"  2, "grades"  [ 98, 100, 102 ] }
         { "_id"  3, "grades"  [ 95, 110, 100 ] }])

;;if upsert and only then
;;  the first document is the q(it cant be filters,i need a document as starting point of the update)
(c-print-all
  (update- :testdb.grades
           (uq {:_id 4}                          ; q=the first document,only if upsert
               {:grades (if- (exists? :grades)
                             (map (fn [:member.]
                                        (if- (>= :member. 100) 100 :member.))
                                   :grades)
                             [98 99 100])}
               {:upsert true})))

(println "----------After upsert update------------")
(c-print-all (q :testdb.grades))