(ns bitcack.core

 (:require [bitcack.serialisation :as serde]
           [bitcack.io :as io]))

(defn- get-by-offset [segment-file offset]
  (some->> (io/read-value-at segment-file offset)
           serde/deserialize
           second))

(comment (get-by-offset "C:\\temp\\db\\0" 263))

(defn- get-by-key [segment-file index key]
  (some->> (get index key)
           (get-by-offset segment-file)))


(defn- set-in-segment! [segment index key value]
  (let [offset (io/file-length segment)
        serialised (serde/serialize key value)]
    (prn "Appending " serialised "at offset" offset)
    (io/append-bytes! segment serialised)
    (assoc index key offset)))

(comment
  (set-in-segment! "C:\\temp\\db\\0" {} "hello" :123a))

(defn- new-segment-name [dir]
  (let [existing-segs (io/find-seg-files-in dir)
        greatest-seg (when (> (count existing-segs) 0)
                           (some->> existing-segs
                                    (map #(str (.getFileName (.toPath %))))
                                    (map #(Integer/parseInt %))
                                    (apply max)
                                    inc
                                    str))]
       (if (nil? greatest-seg)
         (io/join-paths dir "0")
         (io/join-paths dir greatest-seg))))

(comment
  (new-segment-name "C:\\temp\\db")) ; "C:\\temp\\db\\0"


(defn- new-segment? [max-segment-size segment]
  (prn "length of segment: " (io/file-length segment))
  (> (io/file-length segment) max-segment-size))


(defn- create-segment [directory]
  (let [new-segment (new-segment-name directory)]
       (spit new-segment "" :append true)
    {:order (Integer/parseInt (io/file-name new-segment))
     :segment new-segment
     :index {}}))


(defn- insert-new-segment [db-atom directory]
  (let [new-segment (create-segment directory)]
    (swap! db-atom update-in [:segments]
      (fn [old] (vec (sort-by :order #(compare %2 %1)
                       (conj old new-segment)))))))


(defn- increment-index [[index counter] row]
  (let [key (-> row
                serde/deserialize
                first)]
      [(assoc index key counter) (+ counter (count row))]))


(defn- rebuild-index [segment]
  (first (reduce increment-index
          [{} 0]
          (io/map-segment identity segment))))

(comment (rebuild-index "C:\\temp\\db\\0"))

(defn- segment->file-lookup [{:keys [segment index]}]
  (reduce-kv (fn [map key value]
               (assoc map key {:segment segment :offset value}))
    {} index))


(defn- merge-segments [coll]
  (->> (map segment->file-lookup coll)
       reverse
       (apply merge)))


(defn- backup [directory]
  (let [files (io/find-seg-files-in directory)]
       (run! (fn [seg-file]
               (io/rename-file seg-file #(str % ".bak")))
         files)))


(defn- compact [{:keys [options segments]}]
    (let [compacted-segment (io/join-paths (:directory options) "0")
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
  (let [seg-files (io/find-seg-files-in directory)]
    (->> seg-files
         (map io/file-name)
         (map (fn [filename]
                (let [full-path (io/join-paths directory filename)]
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
       (if (= ::tombstone value)
           nil
           value)))


(defn upsert [db key value]
  (let [db-value @db
        segment (get-in db-value [:segments 0 :segment])
        index (get-in db-value [:segments 0 :index])
        db-dir (get-in db-value [:options :directory])
        max-segment-size (get-in db-value [:options :max-segment-size])
        max-segment-count (get-in db-value [:options :max-segment-count])]
    (when (new-segment? max-segment-size segment)
          (prn "Creating new segment.")
          (insert-new-segment db db-dir))
    (when (> (count (:segments db-value)) max-segment-count)
          (prn "Compacting db.")
          (let [compacted-db (compact db-value)]
            (swap! db #(assoc-in %1 [:segments] [%2]) compacted-db)))
    (let [new-index (set-in-segment! segment index key value)]
      (prn "New index: " new-index)
      (swap! db update-in [:segments 0] (fn [m]
                                          (assoc m :index new-index))))))


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
         (upsert db "bbbbbbbbbbbbbbbb" {:arrrrrrrgr 123})
         (upsert db "bbbbbbbbbbbbbbbb" [0 1 2 3 4])
         (lookup db "bbbbbbbbbbbbbbbb")
         (delete! db "bbbbbbbbbbbbbbbb")
         (lookup db "bbbbbbbbbbbbbbbb")
         (deref db))
