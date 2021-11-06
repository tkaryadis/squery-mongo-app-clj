(ns cmql-app-clj.examples.forum32customAccum
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
  (:import (com.mongodb.client MongoClients MongoCollection MongoClient AggregateIterable)
           (com.mongodb MongoClientSettings)
           (org.bson.types ObjectId)
           (java.util Date Calendar)
           (com.mongodb.client.model Indexes)))


(update-defaults :client-settings (-> (MongoClientSettings/builder)
                                      (.codecRegistry clj-registry)
                                      (.build)))

(update-defaults :client (MongoClients/create ^MongoClientSettings (defaults :client-settings)))

;;https://stackoverflow.com/questions/69598277/is-it-possible-to-do-a-mongodb-request-but-return-5-row-with-your-item-in-the-mi

(try (drop-collection :testdb.testcoll) (catch Exception e ""))

(defn add-docs [ndocs start]
  (loop [n start
         docs []]
    (if (= n (+ ndocs start))
      (insert :testdb.testcoll docs)
      (recur (inc n) (conj docs {:name n
                                 :score n})))))


(dotimes [x 1] (add-docs 10 (* x 10)))

(c-print-all (q :testdb.testcoll))


;;;db.books.aggregate([
;{
;  $group :
;  {
;    _id : "$author",
;    avgCopies:
;    {
;      $accumulator:
;      {
;        init: function() {                        // Set the initial state
;          return { count: 0, sum: 0 }
;        },
;        accumulate: function(state, numCopies) {  // Define how to update the state
;          return {
;            count: state.count + 1,
;            sum: state.sum + numCopies
;          }
;        },
;        accumulateArgs: ["$copies"],              // Argument required by the accumulate function
;        merge: function(state1, state2) {         // When the operator performs a merge,
;          return {                                // add the fields from the two states
;            count: state1.count + state2.count,
;            sum: state1.sum + state2.sum
;          }
;        },
;        finalize: function(state) {               // After collecting the results from all documents,
;          return (state.sum / state.count)        // calculate the average
;        },
;        lang: "js"
;      }
;    }
;  }
;}
;])

(c-print-all (q :testdb.testcoll
                (group {:_id nil}
                       {:sum {"$accumulator"
                              {:init "function() {return 0;}"
                               :accumulate "function(state,score){return state+score;}"
                               :accumulateArgs ["$score"],
                               :merge "function(state1,state2) {return state1+state2;}"
                               "lang" "js"}
                              }})))

(c-print-all (q :testdb.testcoll
                (group {:_id nil}
                       {:sum {"$accumulator"
                              {:init "function() {return 0;}"
                               :accumulate "function(state,root,v){return state+root.score+v;}"
                               :accumulateArgs ["$$ROOT",2],
                               :merge "function(state1,state2) {return state1+state2;}"
                               "lang" "js"}
                              }})))


;;TODO NEED SUBTRACK TO BE ==0 ??

(c-print-all (q :testdb.testcoll
                (sort :score)
                (group {:_id nil}
                       {:sum {"$accumulator"
                              {:init "function() {return {found: null, prv: [] , next: []};}"
                               :accumulate "function(state,root,v)
                                            {
                                              if(state.found==null && (root.score-v)==0)
                                              {
                                                return {found: root , prv: state.prv, next: state.next};
                                              }
                                              else if(state.found==null)
                                              {
                                                var prv= state.prv;
                                                if(prv.length==0) prv=[root];
                                                else prv=[prv[prv.length - 1],root];
                                                return {found: state.found , prv: prv, next: state.next};
                                              }
                                              else if(state.next.length<2)
                                              {
                                                var next = state.next;
                                                next.push(root);
                                                return {found: state.found , prv: state.prv, next: next};
                                              }
                                              else return state;
                                            }"
                               :accumulateArgs ["$$ROOT",5],
                               :merge "function(state1,state2) {if(state1.found) return state1; else return state2;}"
                               "lang" "js"}
                              }})))