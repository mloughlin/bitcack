(ns bitcack.io
  (:import [java.io RandomAccessFile FileOutputStream DataOutputStream DataInputStream])
  (:require [clojure.java.io :as jio]))


(defn read-value-at [file offset]
 (with-open [raf (doto (RandomAccessFile. file "r")
                   (.seek offset))]
     (let [ba-len (.readInt raf)
           ba (byte-array ba-len)]
       (.readFully raf ba)
       ba)))

(comment
  (read-value-at "C:\\temp\\db\\0" 0))

(defn append-bytes! [file ^bytes byte-array]
  (let [ba-len (alength byte-array)]
    (with-open [out (DataOutputStream. (FileOutputStream. file true))]
        (.writeInt out ba-len)
        (.write out byte-array))))


(defn find-seg-files-in [dir]
  (let [dir (jio/file dir)]
    (if (.isDirectory dir)
      (->> (file-seq dir)
           (filter #(.isFile %))
           (filter #(neg? (.lastIndexOf (.getName %) ".")))) ; segment files have no extension
      nil)))


(defn file-name [path]
  (-> path
      jio/file
      .toPath
      .getFileName
      str))


(defn file-length [path]
  (-> path
      jio/file
      .length))


(defn join-paths [& paths]
  (str
    (java.nio.file.Paths/get
                             (first paths)
                             (into-array (rest paths)))))

(defn rename-file
  "Renames a file in the same folder.
  Args:
  source (file)
  rename-fn (function that takes the full source file name, returns the new file name)
  copy-option :replace, :copy-attr, :atomic-move"
  [source rename-fn copy-option]
  (let [copy-options {:replace "REPLACE_EXISTING"
                      :copy-attr "COPY_ATTRIBUTES"
                      :atomic-move "ATOMIC_MOVE"}
        dest (rename-fn (.getFileName source))]
    (java.nio.file.Files/move
      (.toPath source)
      (.toPath (jio/file (join-paths (.getParent source) dest)))
      (into-array java.nio.file.CopyOption [(java.nio.file.StandardCopyOption/valueOf (get copy-options copy-option))]))))


(defn- safe-read-int [^DataInputStream dis]
  (try
    (.readInt dis)
    (catch java.io.EOFException _e nil)))


(defn map-segment [f file]
  (letfn [(cack-seq [dis]
            (when-let [len (safe-read-int dis)]
              (let [ba (byte-array len)]
                (.readFully dis ba)
                (cons ba (lazy-seq (cack-seq dis))))))]
    (with-open [dis (DataInputStream. (jio/input-stream (jio/file file)))]
      (into () (map f (cack-seq dis))))))

(comment (map-segment identity "C:\\temp\\db\\0"))
