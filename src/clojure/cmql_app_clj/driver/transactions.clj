(ns cmql-app-clj.driver.transactions
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

(comment
;;transactions cannot run in standalone,replica set is used here
(connect {
          :username "m103-admin"
          :password "m103-pass"
          :host "localhost:27011"
          })

(def client ^MongoClient (get-mongo-client))

(drop-collection :mydb1.food)
(drop-collection :mydb2.bar)


;;if mongo version >= 4.4 , this is not needed,you can create collections/indexes inside the transaction
(insert :mydb1.food
        {:abc 0}
        {:writeConcern { :w "majority" }})

(insert :mydb2.bar
        {:xyz 0}
        {:writeConcern { :w "majority" }})

(def clientSession ^ClientSession (.startSession client))

;;TODO add a simple way to add options using map

;;optional options
;;;TransactionOptions txnOptions = TransactionOptions.builder()
;;        .readPreference(ReadPreference.primary())
;;        .readConcern(ReadConcern.LOCAL)
;;        .writeConcern(WriteConcern.MAJORITY)
;;        .build();

(defn test-trans [clientSession]
  (do (insert :mydb1.food
              {:abc 1}
              clientSession)
      (insert :mydb2.bar
              {:xyz 999}
              clientSession)
      "Inserted into collections in different databases"))

(try (commit clientSession test-trans)
     (catch RuntimeException e (prn e))
     (finally (.close clientSession)))

(c-print-all (q :mydb1.food))
(c-print-all (q :mydb2.bar))

)