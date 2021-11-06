(defn myloop [ar k]
  (loop [k k]
    (if (= k 0)
      ar
      (recur (- k 1)))))