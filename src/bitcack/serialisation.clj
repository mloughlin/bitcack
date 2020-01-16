(ns bitcack.serialisation
  (:require [clojure.string :as str]))


(defn serialize [key value]
  (str key "," value (System/lineSeparator)))


(defn deserialize [input]
  (vec (str/split input #",")))
