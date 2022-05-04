(ns cmql-app-clj.examples.71118128
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
  (:import (com.mongodb.client MongoClients MongoCollection MongoDatabase MongoClient)
           (com.mongodb MongoClientSettings ExplainVerbosity)
           (java.sql Date)))


(update-defaults :client-settings (-> (MongoClientSettings/builder)
                                      (.codecRegistry clj-registry) ;;Remove this if you want to decode in Java Document
                                      (.build)))

(update-defaults :client (MongoClients/create ^MongoClientSettings (defaults :client-settings)))

(try (drop-collection :testdb.testcoll) (catch Exception e ""))
(try (drop-collection :testdb.testcoll2) (catch Exception e ""))
(try (drop-collection :testdb.testcoll3) (catch Exception e ""))
(try (drop-collection :testdb.testcoll4) (catch Exception e ""))
(try (drop-collection :testdb.testcoll5) (catch Exception e ""))


;;project  | environment | timestamp
;----------------------------------------
;project1 | dev         | 1644515845
;project1 | dev         | 1644513211
;project1 | qa          | 1644515542
;project2 | dev         | 1644513692
;project2 | qa          | 1644514822

(defn add-docs [ndocs start]
  (loop [n start
         docs []]
    (if (= n (+ ndocs start))
      (insert :testdb.testcoll docs)
      (let [doc {:project   (str "project" (rand-int 200000))
                 :env       (str "enviroment" (rand-int 10))
                 :timestamp (System/currentTimeMillis)}]
        (recur (inc n) (conj docs doc))))))

(dotimes [x 1000] (add-docs 100 (* x 100)))

;(create-index :testdb.testcoll (index [:project :env :!timestamp]))
;(create-index :testdb.testcoll (index [:project :env :timestamp]))
;(create-index :testdb.testcoll (index [:!timestamp]))

(time (c-take-all (q :testdb.testcoll
                          (sort :!timestamp)
                          (group {:_id {:project :project
                                        :env     :env}}
                                 {:latestDoc (first :ROOT.)})
                          (allow-disk-use)
                          (sort :_id.project)
                          (limit 1)
                          )))

(pprint (.explain (q :testdb.testcoll
                     (sort :!timestamp)
                     (group {:_id {:project :project
                                      :env     :env}}
                               {:latestDoc (first :ROOT.)})
                     (allow-disk-use)
                     (sort :_id.project)
                     (limit 1))))

#_(time (.toCollection (q :testdb.testcoll
                          (group {:_id {:project :project
                                        :env     :env}}
                                 {:latestDoc (max {:timestamp :timestamp
                                                   :doc       :ROOT.})})
                          (allow-disk-use)
                          ;(limit 1)
                          (out :testdb.testcoll2)
                          )))

#_(time (.toCollection (q :testdb.testcoll
                          (sort :project :env :timestamp)
                          (group {:_id {:project :project
                                        :env     :env}}
                                 {:latestDoc (last :ROOT.)})
                          (allow-disk-use)
                          ;(limit 1)
                          (out :testdb.testcoll2)
                          )))

#_(time (.toCollection (q :testdb.testcoll
                          (sort :project :env :!timestamp)
                          (group {:_id {:project :project
                                        :env     :env}}
                                 {:latestDoc (first :timestamp)})
                          (allow-disk-use)
                          ;(limit 1)
                          (out :testdb.testcoll2)
                          )))

#_(time (.toCollection (q :testdb.testcoll
                          (sort :project :env :!timestamp)
                          (group {:_id {:project :project
                                        :env     :env}}
                                 {:timestamp (first :timestamp)})
                          [:!_id {:project :_id.project} {:env :_id.env} :timestamp]
                          (allow-disk-use)
                          ;(limit 1)
                          (out :testdb.testcoll2)
                          )))

#_(time (.toCollection (.hint (q :testdb.testcoll
                                 (sort :project :env :!timestamp)
                                 (group {:_id {:project :project
                                               :env     :env}}
                                        {:timestamp (first :timestamp)})
                                 [:!_id {:project :_id.project} {:env :_id.env} :timestamp]
                                 (allow-disk-use)
                                 ;(limit 1)
                                 (out :testdb.testcoll2))
                              (clj->j-doc (ordered-map "project" 1 "env" 1 "timestamp" -1)))))



#_(time (c-take-all (q :testdb.testcoll
                     (group {:_id {:project :project
                                   :env     :env}}
                            {:timestamp (max :timestamp)})
                     [:!_id {:project :_id.project} {:env :_id.env} :timestamp]
                     (allow-disk-use)
                     (limit 1)
                     ;(out :testdb.testcoll2)
                     )))

#_(time (c-take-all (q :testdb.testcoll
                     (group {:_id {:project :project
                                   :env     :env}}
                            {:doc (max {:timestamp :timestamp
                                        :doc :ROOT.})})
                     ;[:!_id {:project :_id.project} {:env :_id.env} :timestamp]
                     (allow-disk-use)
                     (limit 1)
                     ;(out :testdb.testcoll2)
                     )))

#_(create-index :testdb.testcoll (index [:!timestamp]))

#_(create-index :testdb.testcoll (index [:project :env :timestamp]))


#_(time (c-take-all (q :testdb.testcoll
                        (sort :project :env :timestamp)
                        (group {:_id {:project :project
                                      :env     :env}}
                               {:latestDoc (first :timestamp)})
                        (allow-disk-use)
                        (limit 1)
                     ;(out :testdb.testcoll2)
                        )))



#_(time (c-take-all (q :testdb.testcoll
                     (group {:_id {:env     :env
                                   :project :project}}
                            {:latestDoc (max {:timestamp :timestamp
                                              :doc       :ROOT.})})
                     {:latestDoc :latestDoc.doc}
                     (allow-disk-use)
                     (limit 1)
                     ;(out :testdb.testcoll2)
                     )))

;(create-index :testdb.testcoll (index [:!timestamp]))

#_(time (c-take-all (q :testdb.testcoll
                     (sort :!timestamp)
                     (group {:_id {:project :project
                                   :env     :env}}
                            {:latestDoc (first :ROOT.)})
                     (allow-disk-use)
                     (limit 1)
                     ;(out :testdb.testcoll2)
                     )))

#_(create-index :testdb.testcoll (index [:project :env :!timestamp :_id]))

#_(time (c-take-all (q :testdb.testcoll
                     (sort :project :env :!timestamp :_id)
                     (group {:_id {:project :project
                                   :env     :env}}
                            {:latestDoc (first :ROOT.)})
                     (allow-disk-use)
                     (limit 1)
                     ;(out :testdb.testcoll2)
                     )))



#_(time (c-take-all (q :testdb.testcoll
                     (group {:_id {:project :project
                                   :env     :env}}
                            {:timestamp (max (let [] [:timestamp :_id] ))})
                     ;[:!_id {:project :_id.project} {:env :_id.env} :timestamp]
                     (allow-disk-use)
                     (limit 1)
                     ;(out :testdb.testcoll2)
                     )))

#_(create-index :testdb.testcoll (index [:!timestamp]))
#_(create-index :testdb.testcoll (index [:project :env :timestamp]))

#_(time (c-take-all (q :testdb.testcoll
                     (sort :!timestamp)
                     (group {:_id {:project :project
                                   :env     :env}}
                            {:latestDoc (first (let [] [:timestamp :_id]))})
                     (allow-disk-use)
                     (limit 1)
                     ;(out :testdb.testcoll2)
                     )))

#_(time (c-take-all (q :testdb.testcoll
                     (group {:_id {:project :project
                                   :env     :env}}
                            {:timestamp (max (let [] [:timestamp :_id]))})
                     ;[:!_id {:project :_id.project} {:env :_id.env} :timestamp]
                     (allow-disk-use)
                     ;(limit 1)
                     ;(out :testdb.testcoll2)
                     )))

#_(time (c-take-all (q :testdb.testcoll
                     (group {:_id {:project :project
                                   :env     :env}}
                            {:timestamp (max (let [:v. [:timestamp :_id]] :v.))})
                     ;[:!_id {:project :_id.project} {:env :_id.env} :timestamp]
                     (allow-disk-use)
                     ;(limit 1)
                     ;(out :testdb.testcoll2)
                     )))

#_(time (c-take-all (q :testdb.testcoll
                        (sort :project :env :timestamp)
                        (group {:_id {:project :project
                                      :env     :env}}
                               {:latestDoc (last :ROOT.)})
                        (allow-disk-use)
                        ;(limit 1)
                        ;(out :testdb.testcoll2)
                        )))


#_(time (c-take-all (q :testdb.testcoll
                     (group {:_id {:project :project
                                   :env     :env}}
                            {:timestamp (max {:timestamp :timestamp
                                              :doc :ROOT.})})
                     ;[:!_id {:project :_id.project} {:env :_id.env} :timestamp]
                     (allow-disk-use)
                     (limit 1)
                     ;(out :testdb.testcoll2)
                     )))

#_(time (c-take-all (q :testdb.testcoll
                     (group {:_id {:project :project
                                   :env     :env}}
                            {:timestamp (max {:timestamp :timestamp
                                              :id :_id})})
                     ;[:!_id {:project :_id.project} {:env :_id.env} :timestamp]
                     (allow-disk-use)
                     ;(limit 1)
                     ;(out :testdb.testcoll2)
                     )))



