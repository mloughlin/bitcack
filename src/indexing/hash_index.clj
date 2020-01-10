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
  (let [offset (get index key)]
    (some->> offset
             (read-line-at db)
             deserialize
             second)))


(defn- set-val-internal [db index key value]
  (let [output (serialize key value)
        file-length (.length (io/file db))]
     (swap! index-atom assoc key file-length)
     (spit db output :append true)
     [key value]))


(defn- increment-index [[index i] [key value]]
  (let [len (count (serialize key value))
        new-i (+ i len)]
       [(assoc index key i) new-i]))


(defn get-val [db index-atom key]
  (let [index @index-atom]
    (get-val-internal db index key)))


(defn set-val [{:keys [main-segment segments]} key value]
  (let [index-atom (get segments main-segment)]
    (set-val-internal main-segment index-atom key value)))


(defn rebuild-index [db]
  (with-open [rdr (io/reader db)]
    (->> (line-seq rdr)
         (map deserialize)
         (reduce increment-index [{} 0])
         first)))

(def final-char "b")
(defn- new-segment-name [old-name]
  (let [len (count old-name)
        final-char (subs old-name (- len 1))]
    (if (re-matches #"\d+" final-char)
        (str (subs old-name 0 (- len 1)) (inc (Integer/parseInt final-char)))
        (str old-name "0"))))



(comment (new-segment-name "C:\\temp\\db1"))


(defn compact [source-segment source-index]
  (let [destination-index (atom {})
        destination-segment (new-segment-name source-segment)]
    (->> source-index
         (map (fn [[key value-offset]]
                [key (get-val-internal source-segment source-index key)]))
         (run! (fn [[key value]]
                 (set-val destination-segment destination-index key value))))
    [destination-segment destination-index]))


(defn init-db [segments-dir]
  (do (spit segment "" :append true)
    (let [index-atom (atom (rebuild-index segment))])
    {:main-segment segment
     :segments {segment index-atom}
     :segments-by-age [[segment index-atom]]}))

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
                        :segments {"C:\\temp\\db1" atom}})

         ())
