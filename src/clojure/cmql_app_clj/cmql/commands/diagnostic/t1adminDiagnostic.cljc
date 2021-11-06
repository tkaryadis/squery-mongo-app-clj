(ns cmql-app-clj.cmql.commands.diagnostic.t1adminDiagnostic
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

(prn (get (server-status) :connections))

