(ns cmql-app-clj.examples.forum21reduce
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
        flatland.ordered.map
        clojure.pprint)
  (:refer-clojure)
  (:require [clojure.core :as c]
            [clojure.data.json :as tojson])
  (:import (com.mongodb.client MongoClients MongoCollection MongoClient)
           (com.mongodb MongoClientSettings)
           (org.bson.types ObjectId)
           (java.util Date Calendar)))

(update-defaults :client-settings (-> (MongoClientSettings/builder)
                                      (.codecRegistry clj-registry)
                                      (.build)))

(update-defaults :client (MongoClients/create ^MongoClientSettings (defaults :client-settings)))


(try (drop-collection :testdb.testcoll) (catch Exception e ""))

;;https://stackoverflow.com/questions/68759535/mongodb-aggregation-keep-value-from-previous-document-if-null-or-not-exists#68759535

(def data {
           "_id" "6115150f01d7d0426bcd0390"
           "conf" "conference123",
           "uid" "2dd8b4e3-9dcd-4da6-bc36-aa0988dc9642",
           "log" [
                   {
                    "dt" "2021-08-12T12:33:49.782Z",
                    "connection_quality" 60,
                    "video_bitrate" 150
                    },
                   {
                    "dt" "2021-08-12T12:34:19.781Z",
                    "video_bitrate" 145
                    ;;// connection_quality didn't change so it's not stored
                    },
                   {
                    "dt" "2021-08-12T12:34:30.781Z"
                    "video_bitrate" 130
                    ;// connection_quality didn't change so it's not stored
                    },
                   {
                    "dt" "2021-08-12T12:34:49.787Z"
                    "connection_quality" 100,
                    "video_bitrate" 150
                    },
                   {
                    "dt" "2021-08-12T12:35:19.789Z"
                    "video_bitrate" 160
                    ;// connection_quality didn't change so it's not stored
                    }
                   ]
           })

(insert :testdb.testcoll data)

#_(c-print-all (q :testdb.testcoll
                {:log
                 (get (reduce (fn [:prv-value-logs. :log.]
                                (let [:prv-value. (get :prv-value-logs. 0)
                                      :logs. (get :prv-value-logs. 1)]
                                  (if- (value? :log.connection_quality.)
                                    [:log.connection_quality. (conj :logs. :log.)]
                                    [:prv-value. (conj :logs. (merge :log. {:connection_quality :prv-value.}))])))
                              [60 []]
                              :log)
                      1)}))

(c-print-all (q :testdb.testcoll
                {:log (let [:logDates. (filter (fn [:l.]
                                                 (value? :connection_quality))
                                               :log)])}))


;;-----------------------------------------benchmark--------------------------------------------------------------------

(try (drop-collection :testdb.testcoll) (catch Exception e ""))
(try (drop-collection :testdb.testcoll1) (catch Exception e ""))
(try (drop-collection :testdb.testcoll2) (catch Exception e ""))

(defn get-logs [ndocs]
  (loop [n 0
         d [{
             "dt" (System/nanoTime),
             "connection_quality" 60,
             "video_bitrate"      150}]]
    (if (= n ndocs)
      d
      (recur (inc n) (conj d (if (> (rand) 0.5)
                               {
                                "dt" (+ (System/nanoTime) n)
                                "connection_quality" 60,
                                "video_bitrate"      150}
                               {
                                "dt" (+ (System/nanoTime) n)
                                "video_bitrate" 180
                                }))))))

(defn add-docs [ndocs]
  (loop [n 0
         docs []]
    (if (= n ndocs)
      (do                                                   ;(spit "./skata.txt" (tojson/write-str docs ))
          (insert :testdb.testcoll docs))
      (recur (inc n) (conj docs {:log (get-logs 10000)})))))


(add-docs 1)

(time (.toCollection (q :testdb.testcoll
                        {:log
                         (get (reduce (fn [:prv-value-logs. :log.]
                                        (let [:prv-value. (get :prv-value-logs. 0)
                                              :logs. (get :prv-value-logs. 1)]
                                          (if- (value? :log.connection_quality.)
                                            [:log.connection_quality. (conj :logs. :log.)]
                                            [:prv-value. (conj :logs. (merge :log. {:connection_quality :prv-value.}))])))
                                      [60 []]
                                      :log)
                              1)}
                        (out :testdb.testcoll1))))

(time (.toCollection (q :testdb.testcoll
                        {"$addFields"
                         {"log"
                          {"$map"
                           {"input" "$log",
                            "as" "l",
                            "in" {"$cond"
                                  [{"$eq" [{"$type" "$$l.connection_quality"} "missing"]}
                                   {"dt" "$$l.dt",
                                    "connection_quality"
                                         {"$let"
                                          {"vars"
                                                {"log"
                                                 {"$last"
                                                  {"$filter"
                                                   {"input" "$log",
                                                    "cond"
                                                            {"$and"
                                                             [{"$lt" ["$$this.dt" "$$l.dt"]}
                                                              {"$ne"
                                                               [{"$type" "$$this.connection_quality"}
                                                                "missing"]}]}}}}},
                                           "in" "$$log.connection_quality"}}}
                                   "$$l"]}}}}}
                        (out :testdb.testcoll2))))