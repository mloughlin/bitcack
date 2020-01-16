(ns bitcack.serde
  (:require [clojure.string :as str]))

;; serde - Serialization/Deserialization

(defn serialize [key value]
  (str key "," value (System/lineSeparator)))


(defn deserialize [input]
  (prn "DESER: " input)
  (vec (str/split input #",")))
