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


(defn- new-segment-name [old-name]
  (let [file (io/file old-name)]
    (if (.isDirectory file)
      (->> (file-seq file)
           (filter #(.isFile %))
           (filter #(neg? (.lastIndexOf (.getName %) ".")))
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
       reverse ; merge
       (apply merge)))


(defn- backup [directory]
  (->> (file-seq (io/file directory))
       (filter #(.isFile %))
       (filter #(neg? (.lastIndexOf (.getName %) ".")))
       (run! #(java.nio.file.Files/move
                (.toPath %1)
                (.toPath (io/file (str (.toPath %1) ".bak")))
                (into-array java.nio.file.CopyOption [(java.nio.file.StandardCopyOption/REPLACE_EXISTING)])))))


(defn compact [{:keys [options segments]}]
    (let [compacted-segment (join-paths (:directory options) "0")
          key-values (->> (merge-segments segments)
                          (mapv (fn [[key lookup]]
                                 [key (get-by-offset (:segment lookup) (:offset lookup))])))]
        (backup (:directory options))
        {:segment compacted-segment
         :order 0
         :index (reduce (fn [index [key value]]
                            (set-in-segment! compacted-segment index key value))
                  {} key-values)}))


(defn- harvest-segments [directory]
  (->> (io/file directory)
       file-seq
       (filter #(.isFile %))
       (filter #(neg? (.lastIndexOf (.getName (io/file %)) ".")))
       (map #(str (.getFileName (.toPath %))))
       (map (fn [filename]
              (let [full-path (join-paths directory filename)]
                {:order (Integer/parseInt filename)
                 :segment full-path
                 :index (rebuild-index full-path)})))
       (sort-by :order #(compare %2 %1))
       vec))


(defn get-val
  "Gets the stored value of a given key from the database.
  The implementation checks through each segment in the database
  order until a value is found, and returned."
  [db-atom key]
  (let [{:keys [segments]} @db-atom]
    (some (fn [{:keys [segment index]}]
            (get-by-key segment index key))
      segments)))


(defn set-val [db key value]
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


(defn init-db
  "Initialise the database in the given directory.
   Creates the first database file under the assumption the directory is empty.
   Returns a map:
      - :options {:max-segment-size 2048 :directory \"C:\\temp\\db\" :max-segment-count 10}
      - :segments [{:segment \"C:\\temp\\db\\0\" :index {\"my-key\" 0}}]"
  [options]
  (let [default-config {:max-segment-size 2048 :max-segment-count 50}
        segments (harvest-segments (:directory options))]
    {:options (merge default-config options)
     :segments (if (empty? segments)
                 [(create-segment (:directory options))]
                 segments)}))


(comment (def db (atom (init-db {:directory "C:\\temp\\db"})))
         (set-val db "bbbbbbbbbbbbbbbb" :e)
         (get-val db "bbbbbbbbbbbbbbbb")
         (merge-segments (:segments @db)))
