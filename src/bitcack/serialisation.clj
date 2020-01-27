(ns bitcack.serialisation
  (:require [taoensso.nippy :as nippy]))


(defn serialize
  ([kvp]
   (nippy/freeze kvp))
  ([key value]
   (nippy/freeze [key value])))

(comment
  (serialize "hello" "my-value")
  (serialize ["key" "value pair"]))


(defn deserialize [input]
  (nippy/thaw input))


(comment
  (let [bytes (serialize "ah" 10001)]
    (deserialize bytes)))


(defn- serde-eq [input]
  (assert (= (-> input
               serialize
               deserialize)
             input)))


(comment (run! serde-eq [["key" :keywordabc]
                         ["key" ::namespacedkw]
                         ["key" 100011]
                         ["key" 10000000000001247237237723474327423234234]
                         ["key" "my-string"]
                         ["keyyyy" {:my :map}]
                         ["key" [1 2 :a "yarp"]]
                         ["key" #{:a :b :set-test}]
                         ["bool-key" false]]))
