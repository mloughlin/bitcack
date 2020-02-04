(ns bitcack.core
  (:require [clojure.spec.alpha :as s]))

(s/def ::directory string?)

(s/def ::max-segment-size pos-int?)

(s/def ::max-segment-count pos-int?)

(s/def ::init-options (s/keys :req-un [::directory]
                              :opt-un [::max-segment-size ::max-segment-count]))

(s/def ::key string?)

(s/def ::segment string?)

(s/def ::offset #(>= % 426))
