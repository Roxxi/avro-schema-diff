(ns avro-schema-diff.core
  (:require [roxxi.utils.print :refer :all]
            [roxxi.utils.collections :refer :all])
  (:require [cheshire.core :as json])
  (:import [java.io File InputStream]
           [org.apache.avro Schema Schema$Parser
            Schema$Type
            Schema$Field]))




;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; # Parsing Avro Schemas
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defmulti parse-schema type)
(defmacro make-parse-schema [type]
  (let [type-hint (symbol (str "^" type))]
    `(defmethod parse-schema ~type [src#]
       (let [parser# (Schema$Parser.)]
         (.parse parser# src#)))))

 (make-parse-schema File)
 (make-parse-schema String)
 (make-parse-schema InputStream)

(comment
  "tests"
  (parse-schema "{\"type\":\"string\"}")
  (parse-schema "path/to/file"))

(declare fields-missing fields-type-changed)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; # Dealing with Avro Types
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn schema-type [^Schema avro-schema]
  (.getType avro-schema))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ## Generating Types

(defn- make-avro-type-sym [type]
  (symbol (str "Schema$Type/" (clojure.string/upper-case type))))

(defmacro def-avro-type [type]
  `(def ~(symbol (str "avro-" type)) ~(make-avro-type-sym type)))

(defmacro def-avro-types
  ([type]
     `(def-avro-type ~type))
  ([type & types]
     `(do
        (def-avro-type ~type)
        (def-avro-types ~@types))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ## Generating Type predicats

(defmacro def-schema-type-fn? [type avro-type]
  `(defn ~(symbol (str "avro-" type "?"))
     ~(str "True iff the provided avro-schema is an avro " type ".")
     [^Schema avro-schema#]
     (= (schema-type avro-schema#) ~avro-type)))


(defmacro def-schema-type? [type]
  `(def-schema-type-fn? ~type ~(make-avro-type-sym type)))

(defmacro def-schema-types?
  ([type]
     `(def-schema-type? ~type))
  ([type & types]
     `(do
        (def-schema-type? ~type)
        (def-schema-types? ~@types))))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ## Generating type related functions

(defmacro def-avro-type-helper [type]
  `(do
     (def-avro-type ~type)
     (def-schema-type? ~type)))

(defmacro def-avro-type-helpers [& types]
  `(do
    ~`(def-avro-types ~@types)
    ~`(def-schema-types? ~@types)))
  

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; This generates array-type? boolean-type? bytes-type?
;; functions.
;; Types are listed here:
;; http://avro.apache.org/docs/1.7.4/api/java/index.html
;;
(def-avro-type-helpers
  array
  boolean
  bytes
  double
  enum
  fixed
  float
  int
  long
  map
  null
  record
  string
  union)
  


(defn field-schema [^Schema$Field f]
  (.getSchema f))

;; Note, this will replace the clojure.core/name functionality
;; in this name space.
(defprotocol Named
  (name [_] "Returns the name of the object"))

(extend-protocol Named
  Schema
  (name [schema] (.getName schema))
  Schema$Field
  (name [field] (.getName field))
  Schema$Type
  (name [type] (.getName type)))

(defprotocol AvroRecord
  (fields [schema]
    "Returns the fields of this record")
  (field-names [schema]
    "Returns the field names of this record")
  (field-name=>field-types [schema]
    "Returns a map of field names to their types"))

(extend-type Schema
  AvroRecord
  (fields [schema]
    (when (avro-record? schema)
      (.getFields schema)))
  (field-names [schema]
    (map name (fields schema)))
  (field-name=>field-types [schema]
    (extract-map (fields schema)
                 :key-extractor name
                 :value-extractor #(schema-type (field-schema %))))) 

(defn fields-added [rec1 rec2]
  (let [base-field-names (into #{} (field-names rec1))
        missing-field? (complement #(base-field-names (name %)))]
    (filter missing-field? (fields rec2))))

(defn fields-missing [rec1 rec2]
  (fields-added rec2 rec1))

(defn- type-change-note [name was is]
  {:name name :was (name was) :is (name is)})

(defn field-type-changes [rec1 rec2]
  (let [common-fields (clojure.set/intersection
                       (into #{} (field-names rec1))
                       (into #{} (field-names rec2)))
        name=>type1 (field-name=>field-types rec1)
        name=>type2 (field-name=>field-types rec2)]
    (loop [type-changes []
           field-names common-fields]
      (if (empty? common-fields)
        type-changes
        (let [fname (first field-names)
              type1 (name=>type1 fname)
              type2 (name=>type2 fname)]
          (if (= type1 type2)
            (recur type-changes (rest field-names))
            (recur
             (conj type-changes (type-change-note fname type1 type2))
             (rest field-names))))))))



;; Write tests for the existing code / make sure it works
;; Change the existing reducer to output each file into S3 at the specified locations
;; Remove step 3
;; Ensure step 4 works.
