(ns cmql-app-clj.cmql.qoperators.elemmatch
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

(update-defaults :client-settings (-> (MongoClientSettings/builder)
                                      (.codecRegistry clj-registry) ;;Remove this if you want to decode in Java Document
                                      (.build)))

(update-defaults :client (MongoClients/create ^MongoClientSettings (defaults :client-settings)))


(try (drop-collection :testdb.testcoll) (catch Exception e ""))

(def docs [{"subregion" "Eastern Asia",
           "latlng" [22.25 114.16666666],
           "alpha3Code" "HKG",
           "callingCodes" ["852"],
           "numericCode" "344",
           "area" 1104,
           "altSpellings" ["HK" "香港"],
           "translations"
           {"nl" "Hongkong",
            "pt" "Hong Kong",
            "br" "Hong Kong",
            "it" "Hong Kong",
            "fa" "هنگ‌کنگ",
            "hr" "Hong Kong",
            "fr" "Hong Kong",
            "de" "Hong Kong",
            "es" "Hong Kong",
            "ja" "香港"},
           "demonym" "Chinese",
           "name" "Hong Kong",
           "region" "Asia",
           "languages"
           [{"iso639_1" "en",
             "iso639_2" "eng",
             "name" "English",
             "nativeName" "English"}
            {"iso639_1" "zh",
             "iso639_2" "zho",
             "name" "Chinese",
             "nativeName" "中文 (Zhōngwén)"}],
           "cioc" "HKG",
           "currencies"
           [{"code" "HKD", "name" "Hong Kong dollar", "symbol" "$"}],
           "regionalBlocs" [],
           "population" 7324300,
           "borders" ["CHN"],
           "capital" "City of Victoria",
           "timezones" ["UTC+08:00"],
           "alpha2Code" "HK",
           "topLevelDomain" [".hk"],
           "flag" "https://restcountries.eu/data/hkg.svg",
           "nativeName" "香港",
           "gini" 53.3}])


(insert :testdb.testcoll docs)

(pprint (fq :testdb.testcoll
            (q= :region "Asia")
            (elem-match :languages (q= :iso639_1 "en"))
            (command)))