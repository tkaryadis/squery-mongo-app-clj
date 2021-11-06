(defn myloop [ar k]
  (loop [ar ar
         k k]
    (if (= k 0)
      ar
      (recur (conj_js ar k) (- k 1)))))    ;;conj_js is a dependency that it should be in lib folder