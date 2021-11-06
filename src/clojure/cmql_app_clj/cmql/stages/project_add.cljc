(ns cmql-app-clj.cmql.stages.project-add
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

;;Addfields
;; Replace always except
;; array-> add document =>array with all members that document
;; document->add document => merge documents

;;Project
;; Replace always except
;; array-> add document =>array with all members that document

(try (drop-collection :testdb.testcoll) (catch Exception e ""))

(def doc {
          "mysingle1" 1,
          "mysingle2" 1,
          "mysingle3" 1,

          "myarray1"  [1,2],
          "myarray2"  [1,2],
          "myarray3"  [1,2],

          "myobject1" {"afield" ""},
          "myobject2" {"afield" ""},
          "myobject3" {"afield" ""}
          })

(insert :testdb.testcoll doc)

;;Addfields

;;Addfields
;; Replace always except
;; array-> add document =>array with all members that document
;; document->add document => merge documents

(c-print-all (q :testdb.testcoll
                {
                 :mysingle1 2
                 :mysingle2 [3]
                 :mysingle3 {"afield1" ""}

                 :myarray1 0
                 :myarray2 [3]
                 ;;add object to array = replace all members of the array with that object (mongo works this way)
                 ;:myarray3 {"afield1" ""}
                 ;;true replace with custom notation !
                 :!myarray3 {"afield1" ""}

                 :myobject1 5
                 :myobject2 [3]
                 ;not replace merges the documents
                 ;:myobject3 {"afield1" ""}
                 ;replace
                 :!myobject3 {"afield1" ""}

                 }))

;;Project

;;Project
;; Replace always except
;; array-> add document =>array with all members that document

;;When i add fields with project and the value is constant number
;;(literal- number) even its nested

;;For example
;; {:d (literal- 1)}
;; {:d {"i" (literal- 1)}} ;;needs also literal

(c-print-all (q :testdb.testcoll
                ;;project not replace when    doc->array  , i replace with !notation
                [:!_id

                 ;replace
                 {:mysingle1 (literal 2)}
                 ;replace
                 {:mysingle2 [3]}
                 ;;replace
                 {:mysingle3 {"afield1" ""}}      ;;empty doesnt work seems,i cant add a field with empty docuement as value

                 ;replace
                 {:myarray1 (literal 0)}
                 ;replace
                 {:myarray2 [3]}

                 ;;add object to array = replace all members of the array with that object (mongo works this way)
                 ;{:myarray3 {"afield1" ""}}
                 ;;true replace with custom notation !
                 {:!myarray3 {"afield1" ""}}

                 ;replace
                 {:myobject1 (literal 5)}
                 ;replace
                 {:myobject2 [3]}
                 ;replace
                 {:myobject3 {"afield1" ""}}
                 ]))