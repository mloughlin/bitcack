(ns bitcack.core
 (:import [java.io RandomAccessFile])
 (:require [clojure.java.io :as io]
           [bitcack.serde :as serde]))


(defn- read-line-at [file offset]
 (with-open [raf (doto (RandomAccessFile. file "r")
                   (.seek offset))]
     (.readLine raf)))


(defn- get-by-offset [segment-file offset]
  (some->> (read-line-at segment-file offset)
           serde/deserialize
           second))


(defn- get-by-key [segment-file index key]
  (some->> (get index key)
           (get-by-offset segment-file)))


(defn- write-to-segment! [db key value]
  (spit db
    (serde/serialize key value)
    :append true))


(defn- set-in-segment! [segment index key value]
  (let [offset (.length (io/file segment))]
    (write-to-segment! segment key value)
    (assoc index key offset)))


(defn- join-paths [& paths]
  (str
    (java.nio.file.Paths/get
                             (first paths)
                             (into-array (rest paths)))))


(defn- new-segment-name [old-name]
  (let [file (io/file old-name)]
    (if (.isDirectory file)
      (->> (file-seq file)
           (filter #(.isFile %))
           ((fn [f]
             (if (empty? f)
              (join-paths old-name "0")
              (->> f
                   (map #(str (.getFileName (.toPath %))))
                   (map #(Integer/parseInt %))
                   (apply max)
                   inc
                   str
                   (join-paths old-name))))))
      (let [dir (.getParent file)
            filename (str (.getFileName (.toPath file)))
            new-file (str (inc (Integer/parseInt filename)))]
        (join-paths dir new-file)))))

(comment
  (new-segment-name "C:\\temp\\db") ; "C:\\temp\\db\\0"
  (new-segment-name "C:\\temp\\db\\0")) ; "C:\\temp\\db\\1"


(defn get-val
  "Gets the stored value of a given key from the database.
  The implementation checks through each segment in the database
  order until a value is found, and returned."
  [db-atom key]
  (let [{:keys [segments]} @db-atom]
    (prn segments)
    (some (fn [{:keys [segment index]}]
            (prn "GETTING SEGMENT" segment)
            (get-by-key segment index key))
      segments)))


(defn- new-segment? [max-segment-size segment]
  (> (.length (io/file segment)) max-segment-size))


(defn- create-segment [previous-segment]
  (let [new-segment (new-segment-name previous-segment)]
       (spit new-segment "" :append true)
    {:order (Integer/parseInt (str (.getFileName (.toPath (io/file new-segment)))))
     :segment new-segment
     :index {}}))


(defn- insert-new-segment [db-atom segment]
  (let [new-segment (create-segment segment)]
    (swap! db-atom update-in [:segments]
      (fn [old] (vec (sort-by :order #(compare %2 %1)
                       (conj old new-segment)))))))


(defn set-val [db key value]
  (let [db-value @db
        segment (get-in db-value [:segments 0 :segment] (:segments db-value))
        max-segment-size (get-in db-value [:options :max-segment-size])]
    (when (new-segment? max-segment-size segment)
          (insert-new-segment db segment))
    (swap! db update-in [:segments 0] (fn [{:keys [segment index] :as m}]
                                        (assoc m :index (set-in-segment! segment index key value))))))


(defn- increment-index [[index counter] row]
  (let [key (-> row
                serde/deserialize
                first)]
      [(assoc index key counter) (+ counter (count row))]))


(defn- rebuild-index [db]
  (with-open [rdr (io/reader db)]
    (->> (line-seq rdr)
         (reduce increment-index [{} 0])
         first)))


(defn- segment->file-lookup [{:keys [segment index]}]
  (reduce-kv (fn [map key value]
               (assoc map key {:segment segment :offset value}))
    {} index))


(defn- merge-segments [coll]
  (->> (map segment->file-lookup coll)
       reverse ; merge
       (apply merge)))


(defn compact [{:keys [options segments]}]
  (let [next-segment (new-segment-name (:directory options))]
    (->> (drop 1 segments)
         merge-segments
         (map (fn [[key lookup]]
                [key (get-by-offset (:segment lookup) (:offset lookup))]))
         (reduce (fn [index [key value]]
                   (set-in-segment! next-segment index key value))
           {}))))

(defn- harvest-segments [directory]
  (->> (io/file directory)
       file-seq
       (filter #(.isFile %))
       (map #(str (.getFileName (.toPath %))))
       (map (fn [filename]
              (let [full-path (join-paths directory filename)]
                {:order (Integer/parseInt filename)
                 :segment full-path
                 :index (rebuild-index full-path)})))
       (sort-by :order #(compare %2 %1))
       vec))


(defn init-db
  "Initialise the database in the given directory.
   The database is represented as a stack holding pairs of
   segment files and their respective indexes.
   Creates the first database file under the assumption the directory is empty.
   Returns a list, as we need a seq that conj's to the front."
  [options]
  (let [default-config {:max-segment-size 2048}
        segments (harvest-segments (:directory options))]
    {:options (merge default-config options)
     :segments (if (empty? segments)
                 (create-segment (:directory options))
                 segments)}))


(comment (def db (atom (init-db {:directory "C:\\temp\\db" :max-segment-size 20})))
         (set-val db "bbbbbbbbbbbbbbbb" :a)
         (get-val db "bbbbbbbbbbbbbbbb")
         (merge-segments (:segments @db))
         (get-by-offset "C:\\temp\\db\\9" 14)
         (compact @db))
