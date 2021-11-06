(ns cmql-app-clj.driver.transactions
  (:refer-clojure :only [])
  (:use cmql-core.commands.operators.operators
        cmql-core.commands.operators.options
        cmql-core.commands.operators.stages
        cmql-core.commands.administration
        cmql-core.commands.diagnostic
        cmql-core.commands.roles
        cmql-core.commands.users
        cmql-core.commands.read-write
        cmql-core.driverj.client
        cmql-core.driverj.document
        cmql-core.driverj.transactions)
  (:refer-clojure)
  (:require [clojure.core :as c]
            [cmql-core.commands.operators.operators :as o])
  (:import (com.mongodb.client MongoClient ClientSession)))

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