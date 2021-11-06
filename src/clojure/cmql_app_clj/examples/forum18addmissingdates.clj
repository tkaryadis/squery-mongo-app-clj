(ns cmql-app-clj.examples.forum18addmissingdates
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
  (:import (com.mongodb.client MongoClients)
           (com.mongodb MongoClientSettings)))

(update-defaults :client-settings (-> (MongoClientSettings/builder)
                                      (.codecRegistry clj-registry) ;;Remove this if you want to decode in Java Document
                                      (.build)))

(update-defaults :client (MongoClients/create ^MongoClientSettings (defaults :client-settings)))

;;https://stackoverflow.com/questions/68669791/groupby-aggregation-including-missing-dates-in-mongo

(try (drop-collection :testdb.dates) (catch Exception e ""))
(try (drop-collection :testdb.orders) (catch Exception e ""))

(def dates (into [] (map (fn [n] {:date n}) (range 10))))

(insert :testdb.dates dates)

(update- :testdb.dates
  (uq {:date (+ (ISODate "2021-08-01T00:00:00+00:00") (* :date 24 60 60000))}))

(def orders [{:_id 1
              :date (ISODate "2021-08-01T00:00:00+00:00")}
             {:_id 2
              :date (ISODate "2021-08-01T00:00:00+00:00")}
             {:_id 3
              :date (ISODate "2021-08-05T00:00:00+00:00")}
             {:_id 4
              :date (ISODate "2021-08-03T00:00:00+00:00")}])

(insert :testdb.orders orders)

(c-print-all (q :testdb.dates
                (>= :date (date-from-string {:dateString "2021-08-01T00:00:00"}))
                (<= :date (date-from-string {:dateString "2021-08-05T00:00:00"}))
                (lookup-p :orders
                          [:datesDate. :date]
                          [(= (date-to-string {
                                               "format" "%Y-%m-%d"
                                               "date" :datesDate.
                                               })
                              (date-to-string {
                                               "format" "%Y-%m-%d"
                                               "date" :date
                                               }))]
                          :found-orders)
                [:!_id]))





