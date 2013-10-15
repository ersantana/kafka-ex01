(ns kafka-ex01.core
  (:use [clj-kafka.core :only [with-resource]])
  (:require [clj-kafka.producer :as p])
  (:require [clj-kafka.zk :as zk])
  (:require [clj-kafka.consumer.simple :as s])
  (:require [clj-kafka.consumer.zk :as zkc]))


(def zk-config {"zookeeper.connect" "localhost:2181"})
(defn brokers
  "Get brokers from zookeeper"
  [] (zk/brokers zk-config))

(defn controller
  "Get leader node"
  [] (zk/controller zk-config))

(defn topics
  "Get topics"
  [] (zk/topics zk-config))

(def producer (p/producer {"metadata.broker.list" "localhost:9092"
                      "serializer.class" "kafka.serializer.DefaultEncoder"
                      "partitioner.class" "kafka.producer.DefaultPartitioner"}))

(def topic "test")
(defn send-item [item] (p/send-message producer (p/message topic (.getBytes item))))


(def client-id "cli-id")
(def simple-consumer (s/consumer "localhost" 9092 client-id))
(defn get-items [] (let [part 0
                         offset 0
                         fetch-size 4096]
                     (s/messages simple-consumer client-id topic part offset fetch-size)))

(defn do-for-each [item] (-> item :value (String.) prn))

(defn consume-item-batch-and-process [batch-size]
  (let [consumer-config (merge zk-config
                               {"group.id" "clj-kafka.consumer"
                                "auto.offset.reset" "smallest"
                                "zookeeper.session.timeout.ms" "400"
                                "zookeeper.sync.time.ms" "200"
                                "auto.commit.interval.ms" "1000"
                                "auto.commit.enable" "true"})]
    (with-resource [consumer (zkc/consumer consumer-config)]
      zkc/shutdown
      (doall
       (pmap do-for-each (take batch-size (zkc/messages consumer [topic])))))))

(def switch (atom true)) ; a switch to stop workers

(defn run! []
  (while @switch
    (Thread/sleep 1000)
    (do (consume-item-batch-and-process 2))))
