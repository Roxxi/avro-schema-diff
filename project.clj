(defproject com.onekingslane.danger/avro-schema-diff "0.1.0"
  :description "A Clojure library designed to diff any two Avro Record Schemas and provide a summary of the differences."
  :url "https://github.com/okl/danger-avro-schema-diff"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.0"]
                 [org.apache.avro/avro "1.7.4"]
                 [com.onekingslane.danger/clojure-common-utils "0.0.19"]
                 [cheshire "5.2.0"]]
  :warn-on-reflection true)
