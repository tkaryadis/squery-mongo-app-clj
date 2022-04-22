(ns cmql-app-clj.cmql.interop)


;;Interop
;;1)use mql in cmql queries
;;2)use cmql with drivers that except mql


;;1)use mql inside cmql queries (rarely used, because cmql covers mql operators)
;;  - aggregate operators doesnt need any interop (stages or operators of mql can be used raw)
;;  - query operators/update operators must be inside a block of  f or u
;;    (this block will add a special internal symbol "$___q___" so cmql knows that its not aggregate operator)


;;2)use cmql with the drivers
;;  - 3 argument operators   f,u,p  that i can put inside them cmql code
;;  - and d to convert to a Java document

;; example with u,d   the f,p works the same way
;;(.updateOne coll
;            (d {:a 2})
;            (u (+_ :a 100)
;               (set_ :results [1 2 3])))