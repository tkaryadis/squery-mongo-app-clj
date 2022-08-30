(ns squery-mongo-app-clj.js-server-side.ex0-use-js-and-deps
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


(update-defaults :client-settings (-> (MongoClientSettings/builder)
                                      (.codecRegistry clj-registry) ;;Remove this if you want to decode in Java Document
                                      (.build)))

(update-defaults :client (MongoClients/create ^MongoClientSettings (defaults :client-settings)))

(try (drop-collection :testdb.testcoll) (catch Exception e ""))

(def docs [{:_id 1 :mynumber 1 :mystring "a" :myarray [1] :myobject {:a 1}}
           {:_id 2 :mynumber 2 :mystring "b" :myarray [2] :myobject {:a 2}}
           {:_id 3 :mynumber 3 :mystring "c" :myarray [3] :myobject {:a 3}}
           {:_id 4 :mynumber 4 :mystring "d" :myarray [4] :myobject {:a 4}}])

(insert :testdb.testcoll docs)

;;squery provide 2 ways to use js-server-code
;;1)squery wrappers(see ex1-use-js-wrappers)
;;2)wisp,a Clojurescript like language with js data structures (see ex2-use-wisp)

;;Even if we dont use the 2 above,we still use squery to combine dependencies in our js code
;;2 examples here one without dependecies the other with dependencies

;;needed only if we dont have js/assoc_js.js (a standalone function)
;;and we have js/lib/assoc_js.js or js/lib/assoc_js.wisp

(compile-functions :assoc_js :dissoc_js :assoc_dissoc_js)

(c-print-all (q :testdb.testcoll
                {:myobject (ejs :assoc_js
                                [:myobject "b" 2])}))

(println "-----------------------------Using deps---------------------------------------------------------")

;;Here we will call 1 function js/lib/assoc_dissoc_js,that depends on 2 functions, js/lib/assoc_js and js/lib/dissoc_js
;;squery will use the information on assoc_dissoc_js.deps and combine those function,creating a standalone
;;js/assoc_dissoc_js.js function that we can use (this is done above with compile-functions call)

;;this will remove "a" key,and will assoc "b" key with value 2
(c-print-all (q :testdb.testcoll
                {:myobject (ejs :assoc_dissoc_js
                                [:myobject "a" "b" 2])}))

;;If you look at the auto-generated js/assoc_dissoc_js.js it will have its dependencies inside the body function
;;The reason to do this(not always have standalone functions),is to avoid code repetition,and perfomance problems

;function assoc_dissoc_js (o,remove_key,add_key,add_value)
;{
;var assoc_js = function assoc_js(o, k, v)
;{
;  o[k]=v;
;  return o;
;};
;var dissoc_js = function dissoc_js(obj,k)
;{
;  delete obj[k];
;  return obj;
;}
;  o = dissoc_js(o,remove_key);
;  o = assoc_js(o,add_key,add_value);
;  return o;
;}