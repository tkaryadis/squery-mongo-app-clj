(ns cmql-app-clj.quickstart_clojure.quick-commands-methods
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
           (com.mongodb MongoClientSettings)
           (java.util Collections Random ArrayList)
           (quickstart_java.models Grade Score)
           (org.bson RawBsonDocument Document)
           (org.bson.types ObjectId)
           (com.mongodb.client.model FindOneAndReplaceOptions ReturnDocument Projections UpdateOptions FindOneAndUpdateOptions)))

;  The Java quickstart https://github.com/mongodb-developer/java-quick-start in cmql-j using the Commands API
;  before starting , load the data (they are in the cmql,app folder,the directory named data-test)
;  cd data-test
;  mongorestore sample_training/.
;  (install mongorestore if you dont have it https://docs.mongodb.com/database-tools/installation/installation-linux/)
;  (sample data part of the mongod-atlas datasets from https://github.com/huynhsamha/quick-mongo-atlas-datasets)
;

;;--------------------------------------insert data(create)-------------------------------------------------------------
(def random (Random.))

(defn generateNewGrade [student-id class-id]
  {:_id        (ObjectId.)
   :student_id student-id
   :class_id   class-id
   :scores     [{:type "exam" :score (* (.nextDouble random) 100)}
                {:type "quiz" :score (* (.nextDouble random) 100)}
                {:type "homework" :score (* (.nextDouble random) 100)}
                {:type "homework" :score (* (.nextDouble random) 100)}]})

(defn insert-commands []
  (insert :sample_training.grades (generateNewGrade 10000. 1.))
  (println "One grade inserted for studentId 10000.")
  (insert :sample_training.grades
          (mapv (fn [n] (generateNewGrade 10001. (double n))) (range 1 11))
          {:ordered false})
  (println "Ten grades inserted for studentId 10001."))


(defn insert-methods []
  (let [grades (.getCollection (.getDatabase (defaults :client) "sample_training") "grades")]
    (.insertOne grades (d (generateNewGrade 10000. 1.)))
    (println "One grade inserted for studentId 10000.")
    (insert grades
            (mapv (fn [n] (d (generateNewGrade 10001. (double n)))) (range 1 11))
            {:ordered false})
    (println "Ten grades inserted for studentId 10001.")))

;;--------------------------------------find data(read)-----------------------------------------------------------------

;;fq is called like a command,but internally the driver method is used because it returns cursor
(defn read-commands []
  (println (.first (fq :sample_training.grades (= :student_id 10000))))
  (c-print-all (fq :sample_training.grades (= :student_id 10000)))

  (dorun (map pprint (c-take-all (fq :sample_training.grades (>= :student_id 10000)))))

  (let [c (fq :sample_training.grades
              (= :student_id 10001)
              (<= :class_id 5)
              [:!_id :class_id :student_id]
              (sort :!class_id)
              (skip 2)
              (limit 2))]
    (println "Student sorted, skipped, limited and projected: ")
    (dorun (map println (c-take-all c)))))

(defn read-methods []
  (let [grades (.getCollection (.getDatabase (defaults :client) "sample_training") "grades")]
    (println (.first (.find grades (f (= :student_id 10000)))))
    (c-print-all (.find grades (f (= :student_id 10000))))
    (dorun (map #(println (.toString %)) (c-take-all (.find grades (f (>= :student_id 10000))))))
    (let [c (-> (.find grades (f (= :student_id 10001) (<= :class_id 5)))
                (project-a [:!_id :class_id :student_id])
                (sort-a [:!class_id])
                (.limit 2))]
      (println "Student sorted, skipped, limited and projected: ")
      (dorun (map println (c-take-all c))))))

;;-------------------------------------aggregate(aggregate framework)---------------------------------------------------

;;q is called like a command,but internally the driver method is used because it returns cursor
(defn aggregate-commands []
  (println "==> 3 most densely populated cities in Texas")
  (c-print-all (q :sample_training.zips
                  (= :state "TX")
                  (group :city
                         {:totalPop (sum :pop)})
                  (sort :!totalPop)
                  (limit 3)))
  (println "==> 3 most popular tags and their posts titles")
  (c-print-all (q :sample_training.posts
                  (unwind :tags)
                  (group :tags
                         {:count (sum 1)
                          :titles (conj-each :title)})
                  (sort :!count)
                  (limit 3)
                  [{:tag :tags} :count :titles])))

(defn aggregate-methods []
  (let [db (.getDatabase (defaults :client) "sample_training")
        zips (.getCollection (.getDatabase (defaults :client) "sample_training") "zips")
        posts (.getCollection (.getDatabase (defaults :client) "sample_training") "posts")]
    (println "==> 3 most densely populated cities in Texas")
    (c-print-all (.aggregate zips
                             (p (= :state "TX")
                                (group :city
                                       {:totalPop (sum :pop)})
                                (sort :!totalPop)
                                (limit 3))))
    (println "==> 3 most popular tags and their posts titles")
    (c-print-all (.aggregate posts
                             (p (unwind :tags)
                                (group :tags
                                       {:count (sum 1)
                                        :titles (conj-each :title)})
                                (sort :!count)
                                (limit 3)
                                [{:tag :tags} :count :titles])))))

;;----------------------------------update------------------------------------------------------------------------------

;;All updates in CMQL use pipelines,for updates with update operators
(defn update-commands []
  ;;update one,here command like call is used here
  (update- :sample_training.grades
           (uq (= :student_id 10000)
               {:comment "You should learn MongoDB!"}
               {:multi false}))
  (println "=> Updating the doc with {\"student_id\":10000}. Adding comment.")
  (println (.first (fq :sample_training.grades (= :student_id 10000))))

  ;;upsert
  (println (update- :sample_training.grades
                    (uq {:student_id  10002.
                         :class_id 10.}
                        {:comments (if- (array? :comments)
                                        (conj :comments "You will learn a lot if you read the MongoDB blog!")
                                        ["You will learn a lot if you read the MongoDB blog!"])}
                        {:upsert true})))
  (println "=> Upsert document with {student_id:10002.0, class_id : 10.0} because it doesn't exist yet.")
  (println (.first (fq :sample_training.grades (= :student_id 10002.))))

  ;;update many documents
  (update- :sample_training.grades
           (uq (= :student_id 10001)
               {:comments (if- (array? :comments)
                               (conj :comments "You will learn a lot if you read the MongoDB blog!")
                               ["You will learn a lot if you read the MongoDB blog!"])}))
  (println "=> Updating all the documents with {student_id:10001}")

  ;;findOneAndUpdate (findAndModify cmql command will be used)
  (prn (find-and-modify :sample_training.grades
                        (= :student_id 10000)
                        {:x (if- (exists? :x) (+ :x 10) 10)
                         :news_class_id :class_id
                         :scores (assoc-in :scores [0 "score"] (* (get-in :scores [0 "score"]) 2))
                         :comments (if- (array? :comments)
                                        (conj-distinct (conj-distinct :comments "This comment is uniq")
                                                       "This comment is uniq")
                                        ["This comment is uniq"])}
                        (unset :class_id)
                        {:new true} ;;default is false,and returns the old document as :value
                        ))
  (println "=> Updating all the documents with {student_id:10001}"))


;;// update many documents
;            filter = eq("student_id", 10001);
;            updateResult = gradesCollection.updateMany(filter, updateOperation);
;            System.out.println("\n=> Updating all the documents with {\"student_id\":10001}.");
;            System.out.println(updateResult);

(defn update-methods []
  (let [grades (.getCollection (.getDatabase (defaults :client) "sample_training") "grades")]
    ;;pipeline update
    (.updateOne grades
                (f (= :student_id 10000))
                (p {:comment "You should learn MongoDB!"}))
    (println "=> Updating the doc with {\"student_id\":10000}. Adding comment.")
    (println (.first (.find grades (f (= :student_id 10000)))))
    (println (.updateOne grades
                         (d {:student_id  10002. :class_id 10.})
                         (p {:comments (if- (array? :comments)
                                            (conj :comments "You will learn a lot if you read the MongoDB blog!")
                                            ["You will learn a lot if you read the MongoDB blog!"])})
                         (o (UpdateOptions.)
                            {:upsert true})))
    ;;same not pipeline (cMQL doesnt wrap update operators,for updates it uses pipelines)
    (println (.updateOne grades
                         (d {:student_id  10002. :class_id 10.})
                         (d {"$push" {:comments "You will learn a lot if you read the MongoDB blog!"}})
                         (o (UpdateOptions.)
                            {:upsert true})))
    (println "=> Upsert document with {student_id:10002.0, class_id : 10.0} because it doesn't exist yet.")
    (println (.first (.find grades (f (= :student_id 10002.)))))
    ;;update many documents
    (.updateMany grades
                (f (= :student_id 10001))
                (p {:comments (if- (array? :comments)
                                   (conj :comments "You will learn a lot if you read the MongoDB blog!")
                                   ["You will learn a lot if you read the MongoDB blog!"])}))
    (println "=> Updating all the documents with {student_id:10001}")
    (.findOneAndUpdate grades
                       (f (= :student_id 10000))
                       (p {:x (if- (exists? :x) (+ :x 10) 10)
                           :news_class_id :class_id
                           :scores (assoc-in :scores [0 "score"] (* (get-in :scores [0 "score"]) 2))
                           :comments (if- (array? :comments)
                                          (conj-distinct (conj-distinct :comments "This comment is uniq")
                                                         "This comment is uniq")
                                          ["This comment is uniq"])}
                          (unset :class_id))
                       (o (FindOneAndUpdateOptions.)
                          {:returnDocument ReturnDocument/AFTER})) ;;default is to return the doc before the update
    (println "=> Updating all the documents with {student_id:10001}")))

;;The above updates were pipeline updates,if we want to use update operators we can write MQL raw,cMQL doesnt wrap those
;;for example for the first update we could use
#_(.updateOne (f (= :student_id 10000))
              (d {"$set" {:comment "You should learn MongoDB!"}}))

;;----------------------------------pojo--------------------------------------------------------------------------------

(defn pojo-insert-read-commands []
  (let [db (.withCodecRegistry (.getDatabase ^MongoClient (defaults :client) "sample_training") pojo-registry)
        grades-coll (.withCodecRegistry ^MongoCollection (.getCollection db "grades" Grade) pojo-registry)]
    (insert grades-coll (-> (Grade.)
                            (.setStudentId 10003.)
                            (.setClassId 10.)
                            (.setScores (Collections/singletonList (-> (Score.)
                                                                       (.setType "homeword")
                                                                       (.setScore 50.))))))
    (println "Grade inserted.")
    (let [grade (.first (fq grades-coll (= :student_id 10003.)))]
      (println "Grade Found = " (.toString grade))
      (let [newScores (ArrayList. (.getScores grade))
            _ (.add newScores (-> (Score.) (.setType "exam") (.setScore 42.)))
            _ (.setScores grade newScores)
            ;;use of the driver method with cmql arguments only
            ;;cmql-j findAndModify cant take as argument a custom class
            updatedGrade (.findOneAndReplace grades-coll
                                             (f (= :_id (.getId grade)))
                                             grade
                                             (o (FindOneAndReplaceOptions.)
                                                {:returnDocument ReturnDocument/AFTER}))]
        (println "Grade replaced = " (.toString updatedGrade))
        (println "Delete results = " (delete grades-coll (dq (= :_id (.getId grade))
                                                             {:limit 1})))))))

(defn pojo-insert-read-methods []
  (let [db (.withCodecRegistry (.getDatabase ^MongoClient (defaults :client) "sample_training") pojo-registry)
        grades-coll (.withCodecRegistry ^MongoCollection (.getCollection db "grades" Grade) pojo-registry)]
    (.insertOne grades-coll (-> (Grade.)
                             (.setStudentId 10003.)
                             (.setClassId 10.)
                             (.setScores (Collections/singletonList (-> (Score.)
                                                                        (.setType "homeword")
                                                                        (.setScore 50.))))))
    (println "Grade inserted.")
    (let [grade (.first (.find grades-coll (f (= :student_id 10003.))))]
      (println "Grade Found = " (.toString grade))
      (let [newScores (ArrayList. (.getScores grade))
            _ (.add newScores (-> (Score.) (.setType "exam") (.setScore 42.)))
            _ (.setScores grade newScores)
            ;;use of the driver method with cmql arguments only
            ;;cmql-j findAndModify cant take as argument a custom class
            updatedGrade (.findOneAndReplace grades-coll
                                             (f (= :_id (.getId grade)))
                                             grade
                                             (o (FindOneAndReplaceOptions.)
                                                {:returnDocument ReturnDocument/AFTER}))]
        (println "Grade replaced = " (.toString updatedGrade))
        (println "Delete results = " (.deleteOne grades-coll (f (= :_id (.getId grade)))))))))

;;-----------------------------------delete-----------------------------------------------------------------------------

(defn delete-commands []
  (pprint (delete :sample_training.grades
                  (dq (= :student_id 10000.) {:limit 1})
                  (dq (= :student_id 10002.) {:limit 1})
                  (dq (= :student_id 10000.))))    ;;we could do deleteMany in the top dq but we follow the tutorial
  (pprint (drop-collection :sample_training.grades)))

(defn delete-methods []
  (let [grades (.getCollection (.getDatabase (defaults :client) "sample_training") "grades")]
    (println (.deleteOne grades (f (= :student_id 10000.))))
    (println (.findOneAndDelete grades (f (= :student_id 10002.))))
    (println (.deleteMany grades (f (= :student_id 10000.))))
    (.drop grades)))

;;-------------------------------------Call the commands---------------------------------------------------------

;;cMQL commands(corrensponds to MQL commands runCommand(...)) looks like code,and they are easy to use
(defn run-commands []
  ;;here randomly we choose to get Documents that are Clojure maps
  (update-defaults :client-settings (-> (MongoClientSettings/builder)
                                        (.codecRegistry clj-registry) ;;Remove this if you want to decode in Java Document
                                        (.build)))

  (update-defaults :client (MongoClients/create ^MongoClientSettings (defaults :client-settings)))

  (c-print-all (.listDatabases (defaults :client)))

  (insert-commands)
  (read-commands)
  (aggregate-commands)
  (update-commands)
  (pojo-insert-read-commands)
  (delete-commands))

;;-------------------------------------Call the methods---------------------------------------------------------

;;here we use the driver methods,with cMQL arguments
(defn run-methods []
  ;;here randomly we choose to get Documents that are Clojure maps
  (update-defaults :client-settings (-> (MongoClientSettings/builder) (.build)))

  (update-defaults :client (MongoClients/create ^MongoClientSettings (defaults :client-settings)))

  (c-print-all (.listDatabases (defaults :client)))

  (insert-methods)
  (read-methods)
  (aggregate-methods)
  (update-methods)
  (pojo-insert-read-methods)
  (delete-methods))

;;;-------------------------------Run  from here---------------------------------------------------------------------

;;commands in cmql are programmable and easy to use in cMQL (so 2 implementations are made for 2 ways to use cMQL)
;(run-commands)
;(run-methods)

