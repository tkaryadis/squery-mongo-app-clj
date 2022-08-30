(ns squery-mongo-app-clj.js-server-side.ex1-js-wrappers
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
  (:import (com.mongodb.client MongoClients MongoCollection MongoDatabase MongoClient)
           (com.mongodb MongoClientSettings)))

;;Before starting write the 4 js functions on js/lib OR on js (squery-mongo-app-clj has them already)

;;we need this only if we dont have the bellow functions in js directory
;;for example we have them in js/lib as js files or wisp files,and we need to make them standalone functions
(defn compile-wrappers []
  (compile-functions :assoc_js :dissoc_js :get_js :conj_js ))

(defn assoc-js [o k v]
  (njs :assoc_js [o k v]))

(defn dissoc-js [o k]
  (njs :dissoc_js [o k]))

(defn get-js [o k]
  (njs :get_js [o k]))

(defn conj-js [o v]
  (njs :conj_js [o v]))