(ns kafka-ex01.core
(:use clj-kafka.producer)
(:use clj-kafka.consumer.simple))

(def p (producer {"zk.connect" "localhost:2181"}))

(defn send-item [item] (send-messages p "test" (message (.getBytes item))))

(def c (consumer "localhost" 9092))
(def f (fetch "test" 0 0 4096))
(messages c f)

(defn get-item [] (messages c f))
