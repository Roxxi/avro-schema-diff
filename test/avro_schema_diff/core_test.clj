(ns avro-schema-diff.core-test
  (:use clojure.test
        clojure.java.io
        roxxi.utils.collections
        [avro-schema-diff.core :exclude [name]])
  (:require [cheshire.core :as json]))



(defn- test-file [filename]
  (file (str "test/avro_schema_diff/resources/" filename)))

(defn- field-name [f]
  (avro-schema-diff.core/name f))

(deftest parse-schema-test []
  (testing "Parsing of schemas"
    (testing "from a string"
      (is (avro-string? (parse-schema "{\"type\":\"string\"}"))))
    (testing "from a file"
      (is (avro-string? (parse-schema (test-file "parse-schema-from-file.json")))))
    (testing "from an InputStream"
      "I have no idea how to test from an input stream...")))

(def schemas
  {:empty {"type" "record", "name" "empty", "fields" []}
   :fields3 {"type" "record",
             "name" "fields3",
             "fields" [{"name" "field1","type" "string"},
                       {"name" "field2","type" "int"},
                       {"name" "field3","type" "float"}]}
   :fields5 {"type" "record", "name" "fields5",
             "fields" [{"name" "field1","type" "string"},
                       {"name" "field2","type" "int"},
                       {"name" "field3","type" "float"}
                       {"name" "field4","type" "int"}
                       {"name" "field5","type" "string"}]}
   :fieldsShuffle3 {"type" "record", "name" "fieldsShuffle3",
                    "fields" [{"name" "field2","type" "int"},
                              {"name" "field1","type" "string"},
                              {"name" "field3","type" "float"}]}})

(def s (project-map schemas :value-xform  #(parse-schema (json/generate-string %))))

 
(deftest fields-added-test []
  (testing "If we can determine which fields are added between two record schemas"
    (testing "Empty records"
      (is (empty? (fields-added (:empty s) (:empty s)))))
    (testing "A full record and an empty record"
      (is (empty? (fields-added (:fields3 s) (:empty s)))))
    (testing "An empty record and a full record"
      (is (= (map field-name (fields-added (:empty s) (:fields3 s)))
             ["field1" "field2" "field3"])))
    (testing "3 field record and 5 field record"
      (is (= (map field-name (fields-added (:fields3 s) (:fields5 s)))
             ["field4" "field5"])))
    (testing "5 field record and 3 field record"
      (is (empty? (fields-added (:fields5 s) (:fields3 s)))))
    (testing "Identical records with the fields different orders"
      (is (empty? (fields-added (:fieldsShuffle3 s) (:fields3 s)))))
    (testing "Identical records"
      (is (empty? (fields-added (:fields3 s) (:fields3 s))))))

(deftest fields-missing-test []
  (testing "If we can determine which fields are added between two record schemas"
    (testing "Empty records"
      (is (empty? (fields-missing (:empty s) (:empty s)))))
    (testing "A full record and an empty record"
      (is (= (map field-name (fields-missing (:fields3 s) (:empty s)))
             ["field1" "field2" "field3"])))
    (testing "An empty record and a full record"
      (is (empty? (fields-missing (:empty s) (:fields3 s)))))
    (testing "3 field record and 5 field record"
      (is (empty? (fields-missing (:fields3 s) (:fields5 s)))))
    (testing "5 field record and 3 field record"
      (is (= (map field-name (fields-missing (:fields5 s) (:fields3 s)))
             ["field4" "field5"])))
    (testing "Identical records with the fields different orders"
      (is (empty? (fields-missing (:fieldsShuffle3 s) (:fields3 s)))))
    (testing "Identical records"
      (is (empty? (fields-missing (:fields3 s) (:fields3 s)))))))

(deftest fields-changed-test []
  (testing "If we can determine which fields have changed between two schemas"
    (testing "Empty records")
    (testing "A full record and an empty record")
    (testing "An empty record and a full record")
    (testing "Two records with no common fields")
    (testing "Two records with common fields that have the same types")
    (testing "Two records with common fields that have the different types")
    (testing "Identical records")))




             
    