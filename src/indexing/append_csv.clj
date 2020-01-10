(ns indexing.append-csv
 (:require [clojure.java.io :as io]))

(defn- read-value-csv [data key]
  (->> data
     (re-seq (re-pattern (str key "\\,(.*?)\r\n|\n"))) ; CSV Format (w/ newline terminator): "key,value\n."
     last
     last))

(comment (read-value-csv
           "456,{}\r\n123,{hij:123},\r\nyak,{flarp:barf}\r\n456,{hello: 'world'}\r\n"
           "456"))

(defn get-val [db key]
 (let [data (slurp db)]
   (read-value-csv data key)))


(defn set-val [db key value]
 (let [output (str key "," value (System/lineSeparator))]
   (spit db output :append true)
   output))


(comment (set-val "C:\\temp\\append-csv.txt" "https://catscatscats" 1)
         (set-val "C:\\temp\\append-csv.txt" "https://dogdogdog" 0)
         (set-val "C:\\temp\\append-csv.txt" "https://catscatscats" 2)
         (get-val "C:\\temp\\append-csv.txt" "https://catscatscats")
         (get-val "C:\\temp\\append-csv.txt" "https://dogdogdog")
         (set-val "C:\\temp\\append-csv.txt" "https://catscatscats" 3)
         (set-val "C:\\temp\\append-csv.txt" "https://catscatscats" 4)
         (set-val "C:\\temp\\append-csv.txt" "https://catscatscats" 5)
         (get-val "C:\\temp\\append-csv.txt" "https://catscatscats"))
