(ns avro-schema-diff.core-test
  (:use clojure.test
        clojure.java.io
        [avro-schema-diff.core :exclude [name]]))



(defn test-file [filename]
  (file (str "test/avro_schema_diff/resources/" filename)))

(deftest parse-schema-test []
  (testing "Parsing of schemas"
    (testing "from a string"
      (is (avro-string? (parse-schema "{\"type\":\"string\"}"))))
    (testing "from a file"
      (is (avro-string? (parse-schema (test-file "parse-schema-from-file.json")))))
    (testing "from an InputStream"
      "I have no idea how to test from an input stream...")))


(deftest fields-added-test []
  (testing "If we can determine which fields are added between two record schemas"
    (testing "Empty records")
    (testing "A full record and an empty record")
    (testing "An empty record and a full record")
    (testing "3 field record and 5 field record")
    (testing "5 field record and 3 field record")
    (testing "Identical records with the fields different orders")
    (testing "Identical records")))

(deftest fields-missing-test []
  (testing "If we can determine which fields are missing between two record schemas"
    (testing "Empty records")
    (testing "A full record and an empty record")
    (testing "An empty record and a full record")
    (testing "3 field record and 5 field record")
    (testing "5 field record and 3 field record")
    (testing "Identical records with the fields different orders")
    (testing "Identical records")))

(deftest fields-changed-test []
  (testing "If we can determine which fields have changed between two schemas"
    (testing "Empty records")
    (testing "A full record and an empty record")
    (testing "An empty record and a full record")
    (testing "Two records with no common fields")
    (testing "Two records with common fields that have the same types")
    (testing "Two records with common fields that have the different types")
    (testing "Identical records")))




             
    