(ns squery-mongo-app-clj.squery.stages.graphlookup
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
        clojure.pprint)
  (:refer-clojure)
  (:require [clojure.core :as c])
  (:import (com.mongodb.client MongoClients MongoCollection MongoClient)
           (com.mongodb MongoClientSettings)))

(update-defaults :client-settings (-> (MongoClientSettings/builder)
                                      (.codecRegistry clj-registry) ;;Remove this if you want to decode in Java Document
                                      (.build)))

(update-defaults :client (MongoClients/create ^MongoClientSettings (defaults :client-settings)))

;;(defn graphlookup [collection
;                   startWith
;                   connectFromField
;                   connectToField
;                   as
;                   maxDepth
;                   depthField
;                   restrictSearchWithMatch]
;  {
;   "$graphLookup"
;{
;    "from" collection
;    "startWith"  startWith   ;;expression
;    "connectFromField" connectFromField   ;;string
;    "connectToField" connectToField       ;;string
;    "as"  as  ;;string
;    "maxDepth" maxDepth     ;;number
;    "depthField" depthField ;;string
;    "restrictSearchWithMatch" restrictSearchWithMatch       ;;doc
;    }
;   })