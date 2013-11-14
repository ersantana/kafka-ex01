(ns kafka-ex01.core
  (:use [clj-kafka.core :only [with-resource]])
  (:require [clj-kafka.producer :as p])
  (:require [zookeeper :as zk])
  (:require [clj-kafka.zk :as kzk])
  (:require [clj-kafka.consumer.simple :as s])
  (:require [clj-kafka.consumer.zk :as zkc])
  (:gen-class))

(defn brokers
  "Get brokers from zookeeper"
  [zookeeper-cli]
  (prn "brokers:")
  (prn "-> " (kzk/brokers {"zookeeper.connect" zookeeper-cli})))

(defn controller
  "Get leader node"
  [zookeeper-cli]
  (prn "leader:")
  (prn "-> " (kzk/controller {"zookeeper.connect" zookeeper-cli})))

(defn topics
  "Get topics"
  [zookeeper-cli]
  (let [topics (kzk/topics  {"zookeeper.connect" zookeeper-cli})]
    (if (= 0 (count topics)) (prn "no topic") (do (prn "topics:")(prn topics)))))

(defn groups
  "Get groups"
  [zookeeper-cli]
  (let [cli (zk/connect zookeeper-cli)
        node "/consumers"]
    (if (zk/exists cli node) (do (prn "groups:")(prn (zk/children cli node))) (prn "no group"))))
  
(defn get-offset
  "Get the offset of a topic"
  ([zookeeper-cli topic group-id] (get-offset zookeeper-cli topic group-id 0))
  ([zookeeper-cli topic group-id partition-id]
  (let [cli (zk/connect zookeeper-cli)
        node (str "/consumers/" group-id "/offsets/" topic "/0")
        node-exists? (zk/exists cli node)]
    (if node-exists? (String. (:data (zk/data cli node))) 0))))

(defn send-item [kafka-cli topic message]
  (let [producer (p/producer {"metadata.broker.list" kafka-cli
                      "serializer.class" "kafka.serializer.DefaultEncoder"
                      "partitioner.class" "kafka.producer.DefaultPartitioner"})]
    (p/send-message producer (p/message topic (.getBytes message)))))

(defn get-items [kafka-cli topic group-id zookeeper-cli]
  (let [part 0
        offset 0
        fetch-size 4096
        client-id "cli-id"
        simple-consumer (s/consumer (-> kafka-cli (clojure.string/split #":") first) (-> kafka-cli (clojure.string/split #":") second read-string) client-id)
        messages (s/messages simple-consumer client-id topic part offset fetch-size)]
    (prn "messages")
    (prn "count= " (count messages))
    (prn "offset=" (get-offset zookeeper-cli topic group-id))
    (prn "payload: ")
    (clojure.pprint/pprint (map #(-> % (:value %) (String.)) messages))))

(defn -main [& args]
  (prn "use help to learn how to use this tool")
  (case (first args)
    "get-topics" (topics (second args))
    "get-leader" (controller (second args))
    "get-brokers" (brokers (second args))
    "get-groups" (groups (second args))
    "get-items" (get-items (nth args 1) (nth args 2) (nth args 3) (nth args 4))
    "send-item" (send-item (nth args 1) (nth args 2) (nth args 3))
    "help" (do 
                (prn "arg 0 : action -> get-items|get-topics|get-leader|get-groups|get-brokers|send-item")
                (prn "GET-ITEMS   - lein run get-items localhost:9092 NOTIFICATIONS-TOPIC NOTIFICATIONS-GROUP localhost:2181")
                (prn "SEND-ITEM   - lein run send-item localhost:9092 topic message")
                (prn "GET-TOPICS  - lein run get-topics localhost:2181")
                (prn "GET-LEADER  - lein run get-leader localhost:2181")
                (prn "GET-GROUPS - lein run get-groups localhost:2181")
                (prn "GET-BROKERS - lein run get-brokers localhost:2181"))
    "default"))

;;-----------

#_(defn do-for-each [item] (-> item :value (String.) prn))

#_(defn consume-item-batch-and-process [batch-size]
  (let [consumer-config (merge  {"zookeeper.connect" zookeeper-cli}
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

#_(def switch (atom true)) ; a switch to stop workers

#_(defn run! []
  (while @switch
    (Thread/sleep 1000)
    (do (consume-item-batch-and-process 2))))

