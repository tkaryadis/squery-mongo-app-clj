(ns cmql-app-clj.js-server-side.bench1-js-wisp
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
  (:import (com.mongodb.client MongoClients MongoCollection MongoDatabase MongoClient)
           (com.mongodb MongoClientSettings)))


(update-defaults :client-settings (-> (MongoClientSettings/builder)
                                      (.codecRegistry clj-registry) ;;Remove this if you want to decode in Java Document
                                      (.build)))

(update-defaults :client (MongoClients/create ^MongoClientSettings (defaults :client-settings)))

(try (drop-collection :testdb.testcoll) (catch Exception e ""))

(defn add-docs [n]
  (let [docs (into [] (take n (repeat {:myarray []})))]
    (insert :testdb.testcoll docs)))

;;add one million documents like above
(dotimes [_ 100] (add-docs 10000))

;;without language on top + no slow things (no equal etc)
;;wisp loop code ~= 1.15 slower
;;Depends on how many times the loop runs i go from 1.2x => 3x

;;with language on top + slow things (like equal etc)
;;Depends on how many times the loop runs i go from 4x => 30x

;;with language on top + no slow things (like equal etc)
;;2.5x-3x

;;Conclusion => wisp is fine if i remove the slow parts,and reduce a bit the size of the language on top

;;TODO re-write wisp core.js,and remove the slow parts (make wisp even more like javascript)
;;for example Equal(x1,x2) is so slow in wisp(check type first and then decides how to compare),
;; it could be made to be just the == js operator
;;if those done it can go like ~2x slower from javascript