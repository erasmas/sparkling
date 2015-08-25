(ns sparkling.ordering
  (:use clojure.test)
  (:require [sparkling.conf :as conf]
            [sparkling.api :as s]
            [sparkling.serialization :as ser]
            [sparkling.comparator])
  (:import (com.esotericsoftware.kryo Kryo)
           (org.apache.spark.serializer KryoRegistrator)
           (org.objenesis.strategy StdInstantiatorStrategy)))

(deftype Registrator []
  KryoRegistrator
  (#^void registerClasses [#^KryoRegistrator _ #^Kryo kryo]
    (try
      (require 'sparkling.serialization)
      (require 'sparkling.comparator)
      (.setInstantiatorStrategy kryo (StdInstantiatorStrategy.))
      (ser/register-base-classes kryo)
      (ser/register kryo (class sparkling.comparator/least-second-value))

      (catch Exception e
        (RuntimeException. "Failed to register kryo!" e)))))

(deftest ordering

  (let [conf (-> (conf/spark-conf)
                 (conf/set-sparkling-registrator)
                 (conf/set "spark.kryo.registrationRequired" "true")
                 (conf/master "local[*]")
                 (conf/app-name "api-test"))]

    (s/with-context c conf

                    (testing
                      "take-ordered returns the first N elements of an RDD using the natural ordering"
                      (is (= (-> (s/parallelize c [[1 -1] [2 -2] [3 -3] [4 -4]])
                                 (s/take-ordered 1))
                             [[1 -1]])))

                    (testing
                      "take-ordered returns the first N elements of an RDD as defined by the specified comparator"
                      (is (= (-> (s/parallelize c [[1 -1] [2 -2] [3 -3] [4 -4]])
                                 (s/take-ordered 1 sparkling.comparator/least-second-value))
                             [[4 -4]])))
                    )))
