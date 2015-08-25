(ns sparkling.comparator
  (:gen-class))

(def least-second-value (comparator (fn [[_ y1] [_ y2]] (< y1 y2))))