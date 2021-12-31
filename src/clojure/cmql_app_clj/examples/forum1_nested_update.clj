(ns cmql-app-clj.examples.forum1-nested-update
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
        clojure.pprint)
  (:refer-clojure)
  (:require [clojure.core :as c])
  (:import (com.mongodb.client MongoClients MongoCollection MongoClient)
           (com.mongodb MongoClientSettings)))

;;https://developer.mongodb.com/community/forums/t/how-to-update-only-specified-array-elements-from-nested-array-present-in-document/8577

(update-defaults :client-settings (-> (MongoClientSettings/builder) (.codecRegistry clj-registry) (.build)))

(update-defaults :client (MongoClients/create ^MongoClientSettings (defaults :client-settings)))

(try (drop-collection :testdb.testcoll) (catch Exception e ""))

(def example-doc
  {"channel"
   {"_id" "Object ID ",
    "name" "switch",
    "formats"
    [{"_id" "Object ID ",
      "formatName" "ISO8583-93",
      "description" "ISO Format",
      "fields"
      [{"name" "0",
        "alias" "MTI",
        "lenght" "4",
        "description" "",
        "type" "FIXED",
        "dataType" "",
        "required" true}],
      "messages"
      [{"_id" "Object ID ",
        "name" "balanceEnquiry",
        "alias" "balanceEnquiry",
        "description"
        "balanceEnquiry Request : Sender Bank -> MessageHub",
        "messageIdentification" "",
        "messageType" "",
        "messageFormat" "",
        "fields"
        [{"name" "DE_0",
          "alias" "MTI",
          "lenght" "4",
          "description" "",
          "type" "FIXED",
          "dataType" ""}
         {"name" "DE_1",
          "alias" "Primary Bitmap",
          "lenght" "8",
          "description" "Primary Bitmap",
          "type" "BIN",
          "dataType" ""}]}
       {"_id" "Object ID ",
        "name" "fundTransfer",
        "alias" "creditTransfer",
        "description"
        "Funds Transfer Request : Sender Bank -> Message Hub",
        "messageIdentification" "",
        "messageType" "",
        "messageFormat" "",
        "fields"
        [{"name" "DE_0",
          "alias" "MTI",
          "lenght" "4",
          "description" "",
          "type" "FIXED",
          "dataType" ""}
         {"name" "DE_1",
          "alias" "Primary Bitmap",
          "lenght" "8",
          "description" "Primary Bitmap",
          "type" "BIN",
          "dataType" ""}]}]}
     {"_id" "Object ID ",
      "formatName" "ISO20022",
      "description" "",
      "fields"
      [{"name" "0",
        "alias" "MTI",
        "lenght" "4",
        "description" "",
        "type" "FIXED",
        "dataType" "",
        "required" true}
       {"name" "1",
        "alias" "Bitmap(s)",
        "lenght" "8",
        "description" "",
        "type" "BIN",
        "dataType" "",
        "required" true}]}]}})

(insert :testdb.testcoll example-doc)

;;HandMade solution
(c-print-all
  (q :testdb.testcoll
     {:channel
      (if- (= :channel.name "switch")
        (merge :channel
               {:formats (map (fn [:format.]
                                (if- (= :format.formatName. "ISO8583-93")
                                  (merge :format. {:messages (map
                                                               (fn [:formatMessage.]
                                                                 (if- (= :formatMessage.name. "balanceEnquiry")
                                                                   (merge :formatMessage. {:alias "newAlias"})
                                                                   :formatMessage.))
                                                               :format.messages.)})
                                  :format.))
                              ::channel.formats)})
        :channel)}))

;;Query
;;get ["formats"] if parent.name=switch  (parent here is the obect i am in ,the channel)
;;get [X_index]  if member at X index has member.formatName ="ISO8583-93"
;;get ["messages"]
;;get [X_index] if member at X index has member.name= "balanceEnquiry"
;;set ["alias"] = "newAlias"

;;TODO on conditions if doesnt exists , exception
;;generated code i tihnk was smaller in the past
(c-print-all
  (q :testdb.testcoll
     {:channel (assoc-in :channel [{:kcond (= :k. "formats") :cond (= :o.name. "switch")}
                                   {:icond (= :v.formatName. "ISO8583-93")}
                                   "messages"
                                   {:icond (= :v.name. "balanceEnquiry")}
                                   "alias"]
                         "newAlias")}))