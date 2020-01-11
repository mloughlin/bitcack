(ns indexing.hash-index
 (:import [java.io RandomAccessFile])
 (:require [clojure.java.io :as io]
           [clojure.string :as str]))


(def spy #(do (println "DEBUG:" %) %))


(defn serialize [key value]
  (str key "," value (System/lineSeparator)))


(defn deserialize [input]
  (vec (str/split input #",")))


(defn- read-line-at [file offset]
 (with-open [raf (doto (RandomAccessFile. file "r")
                   (.seek offset))]
     (.readLine raf)))


(defn- get-val-internal [db index key]
  (some->> (get index key)
           (read-line-at db)
           deserialize
           second))


(defn- write-db [db key value]
  (spit db
    (serialize key value)
    :append true))


(defn- write-index [index-atom db-file key]
  (->> (.length (io/file db-file))
       (swap! index-atom assoc key)))


(defn- set-val-internal [db index-atom key value]
  (do
    (write-db db key value)
    (write-index index-atom db key)
    [key value]))




(defn get-val [db index-atom key]
  (let [index @index-atom]
    (get-val-internal db index key)))


(defn set-val [{:keys [main-segment segments]} key value]
  (let [index-atom (get segments main-segment)]
    (set-val-internal main-segment index-atom key value)))


(defn- increment-index [[index counter] row]
  (let [key (-> row
                deserialize
                first)]
      [(assoc index key counter) (+ counter (count row))]))


(defn rebuild-index [db]
  (with-open [rdr (io/reader db)]
    (->> (line-seq rdr)
         (reduce increment-index [{} 0])
         first)))


(defn- new-segment-name [old-name]
  (let [len (count old-name)
        final-char (subs old-name (- len 1))]
    (if (re-matches #"\d+" final-char)
        (str (subs old-name 0 (- len 1)) (inc (Integer/parseInt final-char)))
        (str old-name "\\" "0"))))


(comment
  (new-segment-name "C:\\temp\\db") ; "C:\\temp\\db1"
  (new-segment-name "C:\\temp\\db1")) ; "C:\\temp\\db2"


(defn compact [source-segment source-index]
  (let [destination-index (atom {})
        destination-segment (new-segment-name source-segment)]
    (->> source-index
         (map (fn [[key value-offset]]
                [key (get-val-internal source-segment source-index key)]))
         (run! (fn [[key value]]
                 (set-val destination-segment destination-index key value))))
    [destination-segment destination-index]))


(defn init-db [segment]
  (do (spit segment "" :append true)
    (let [index-atom (atom (rebuild-index segment))]
      {:main-segment segment
       :segments {segment index-atom}
       :segments-by-age [[segment index-atom]]})))


(comment (init-db (new-segment-name "C:\\temp\\db")))


(comment (def hash-index (atom {}))
         (reset! hash-index (rebuild-index "C:\\temp\\db1"))
         (set-val-internal "C:\\temp\\db1" hash-index "aaaaa" 1)
         (set-val-internal "C:\\temp\\db1" hash-index "bbbbbbbbbb" 4)
         (set-val-internal "C:\\temp\\db1" hash-index "aaaaa" 2)
         (get-val "C:\\temp\\db1" hash-index "aaaaa")
         (get-val "C:\\temp\\db1" hash-index "bbbbbbbbbb")
         (set-val-internal "C:\\temp\\db1" hash-index "aaaaa" 3)
         (set-val-internal "C:\\temp\\db1" hash-index "aaaaa" 4)
         (set-val-internal "C:\\temp\\db1" hash-index "aaaaa" 5)
         (get-val "C:\\temp\\db1" hash-index "aaaaa")
         (deref hash-index)
         (def new-index (second (compact "C:\\temp\\db1" @hash-index "C:\\temp\\db2.txt")))
         (deref new-index)
         (get-val "C:\\temp\\db2.txt" new-index "aaaaa")
         (get-val "C:\\temp\\db2.txt" new-index "bbbbbbbbbb")

         (def segments {:main-segment "C:\\temp\\db1"
                        :segments {"C:\\temp\\db1" atom}}))
