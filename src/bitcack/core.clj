(ns bitcack.core
 (:import [java.io RandomAccessFile])
 (:require [clojure.java.io :as io]
           [bitcack.serialisation :as serialisation]))


(defn- read-line-at [file offset]
 (with-open [raf (doto (RandomAccessFile. file "r")
                   (.seek offset))]
     (.readLine raf)))


(defn- get-by-offset [segment-file offset]
  (some->> (read-line-at segment-file offset)
           serialisation/deserialize
           second))


(defn- get-by-key [segment-file index key]
  (some->> (get index key)
           (get-by-offset segment-file)))


(defn- set-in-segment! [segment index key value]
  (let [offset (.length (io/file segment))]
    (spit segment
      (serialisation/serialize key value)
      :append true)
    (assoc index key offset)))


(defn- join-paths [& paths]
  (str
    (java.nio.file.Paths/get
                             (first paths)
                             (into-array (rest paths)))))

(defn- find-seg-files-in [dir]
  (let [dir (io/file dir)]
    (if (.isDirectory dir)
      (->> (file-seq dir)
           (filter #(.isFile %))
           (filter #(neg? (.lastIndexOf (.getName %) ".")))) ; segment files have no extension
      nil)))


(defn- new-segment-name [dir]
  (let [existing-segs (find-seg-files-in dir)
        greatest-seg (some->> existing-segs
                              (map #(str (.getFileName (.toPath %))))
                              (map #(Integer/parseInt %))
                              (apply max)
                              inc
                              str)]
       (if (nil? greatest-seg)
         (join-paths dir "0")
         (join-paths dir greatest-seg))))

(comment
  (new-segment-name "C:\\temp\\db") ; "C:\\temp\\db\\0"
  (new-segment-name "C:\\temp\\db\\0")) ; "C:\\temp\\db\\1"


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


(defn- increment-index [[index counter] row]
  (let [key (-> row
                serialisation/deserialize
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
       reverse
       (apply merge)))


(defn- backup [directory]
  (let [files (find-seg-files-in directory)]
       (run! (fn [seg-file]
                (let [path (.toPath seg-file)]
                  (java.nio.file.Files/move
                   path
                   (.toPath (io/file (str path ".bak")))
                   (into-array java.nio.file.CopyOption [(java.nio.file.StandardCopyOption/REPLACE_EXISTING)]))))
         files)))


(defn- compact [{:keys [options segments]}]
    (let [compacted-segment (join-paths (:directory options) "0")
          key-values (->> (merge-segments segments)
                          (into [] (map (fn [[key lookup]]
                                          [key (get-by-offset (:segment lookup) (:offset lookup))])))
                          (filter #(not (= ::tombstone (second %)))))]
        (backup (:directory options))
        {:segment compacted-segment
         :order 0
         :index (reduce (fn [index [key value]]
                            (set-in-segment! compacted-segment index key value))
                  {} key-values)}))


(defn- harvest-segments [directory]
  (let [seg-files (find-seg-files-in directory)]
    (->> seg-files
         (map #(str (.getFileName (.toPath %))))
         (map (fn [filename]
                (let [full-path (join-paths directory filename)]
                  {:order (Integer/parseInt filename)
                   :segment full-path
                   :index (rebuild-index full-path)})))
         (sort-by :order #(compare %2 %1))
         vec)))


(defn lookup
  "Gets the stored value of a given key from the database.
  The implementation checks through each segment in the database
  order until a value is found and returned."
  [db-atom key]
  (let [{:keys [segments]} @db-atom
        value (some (fn [{:keys [segment index]}]
                      (get-by-key segment index key))
                segments)]
       (if (= (str ::tombstone) value)
           (do (prn "it's tombestone") nil)
           value)))


(defn upsert [db key value]
  (let [db-value @db
        segment (get-in db-value [:segments 0 :segment])
        max-segment-size (get-in db-value [:options :max-segment-size])
        max-segment-count (get-in db-value [:options :max-segment-count])]
    (when (new-segment? max-segment-size segment)
          (insert-new-segment db segment))
    (when (> (count (:segments db-value)) max-segment-count)
          (swap! db #(assoc-in %1 [:segments] [%2]) (compact db-value)))
    (swap! db update-in [:segments 0] (fn [{:keys [segment index] :as m}]
                                        (assoc m :index (set-in-segment! segment index key value))))))


(defn delete! [db key]
  (upsert db key ::tombstone))


(defn init
  "Initialise the database in the given directory.
   Creates the first database file under the assumption the directory is empty.
   Returns a map:
      - :options {:max-segment-size 2048 :directory \"C:\\temp\\db\" :max-segment-count 10}
      - :segments [{:segment \"C:\\temp\\db\\0\" :index {\"my-key\" 0}}]"
  [options]
  (let [default-config {:max-segment-size 2048 :max-segment-count 50}
        segments (harvest-segments (:directory options))]
    (atom {:options (merge default-config options)
           :segments (if (empty? segments)
                       [(create-segment (:directory options))]
                       segments)})))


(comment (def db (init {:directory "C:\\temp\\db"}))
         (upsert db "bbbbbbbbbbbbbbbb" 'what-up)
         (lookup db "bbbbbbbbbbbbbbbb")
         (delete! db "bbbbbbbbbbbbbbbb")
         (lookup db "bbbbbbbbbbbbbbbb"))
