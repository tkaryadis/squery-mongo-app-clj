(ns squery-mongo-app-clj.squery.stages.windowfields
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
  (:import (com.mongodb.client MongoClients MongoCollection MongoClient)
           (com.mongodb MongoClientSettings)))


(pprint (wfields :state
                 (squery-mongo-core.operators.stages/sort :orderDate)    ;;if in q enviroment no need for namespace
                 {:cumulativeQuantityForState (sum :quantity)
                  :documents ["unbounded" "current"]}))

(pprint (wfields :state
                 (squery-mongo-core.operators.stages/sort :orderDate)    ;;if in q enviroment no need for namespace
                 {:cumulativeQuantityForState (dense-rank)}))