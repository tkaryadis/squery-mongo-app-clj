(ns cmql-app-clj.js-server-side.ex1-use-js-wrappers
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
        clojure.pprint
        cmql-app-clj.server-side-js.ex1-js-wrappers                  ;; wrappers
        )
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

;;see cmql-app-clj.server-side-js.ex1-js-wrappers
(compile-wrappers)

;;With wrappers,and because cMQL allows many nested functions to generate only 1 js function
;;we can code as if it was cMQL code,and its fast because only 1 function runs at server

;;add a key "e" with the "a" value,and remove "a" key after
;;ejs generates a $function operator
(c-print-all (q :testdb.testcoll
                {:updatedobject (ejs (dissoc-js (assoc-js :myobject "e" (get-js :myobject "a"))
                                                "a"))}))

;;cMQL with the njs ejs operators will make the 4 calls 1 function,the below auto-generated will run at the server
;;function (v3,v4,v1,v2,v5)
;{
;  var f1=function get_js(o,k)
;  {
;    return o[k];
;  };
;  var f2=function assoc_js(o, k, v)
;  {
;    o[k]=v;
;    return o;
;  };;
;  var f3=function dissoc_js(obj,k)
;  {
;    delete obj[k];
;    return obj;
;  };
;  return f3(f2(v3,v4,f1(v1,v2)),v5);
;}