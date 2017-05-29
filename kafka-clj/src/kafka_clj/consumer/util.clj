(ns kafka-clj.consumer.util
  (:require
    [kafka-clj.fetch :as fetch]
    [clojure.tools.logging :refer [info error debug]]
    [kafka-clj.metadata :as kafka-metadata]
    [schema.core :as s]
    [kafka-clj.schemas :as schemas])
  (:import (org.apache.kafka.common.requests ListOffsetResponse ListOffsetResponse$PartitionData)
           (kafka_clj.util Util)))


(defn transform-offsets [topic ^ListOffsetResponse offsets-response {:keys [use-earliest] :or {use-earliest true}}]
  "Transforms [{:topic topic :partitions {:partition :error-code :offsets}}]
   to {topic [{:offset offset :partition partition}]}"
  (Util/getPartitionOffsetsByTopic offsets-response topic use-earliest (fn [topicPartition ^ListOffsetResponse$PartitionData partitionData]
                                                                         (let [offsets (.offsets partitionData)
                                                                               error-code (.errorCode partitionData)]
                                                                           (if (zero? error-code)
                                                                             (and (not (nil? offsets)) (not (empty? offsets)))
                                                                             (error "Error when reading list of offsets for " topicPartition ", error code: " error-code))))))

(defn get-offsets [metadata-connector host-address topic partitions]
  {:pre [metadata-connector
         (s/validate schemas/TOPIC-SCHEMA topic)
         (s/validate [schemas/PARITION-SEGMENT] partitions)]}
  "returns [{:topic topic :partitions {:partition :error-code :offsets}}]"
  ;we should send format [[topic [{:partition 0} {:partition 1}...]] ... ]

  (transform-offsets topic
                     (fetch/send-recv-offset-request
                       metadata-connector
                       host-address
                       [[topic partitions]])
                     (:conf metadata-connector)))

(defn get-broker-offsets
  "
   metadata  {\"abc\" [{:host \"localhost\", :port 50738, :isr [{:host \"localhost\", :port 50738}], :id 0, :error-code 0}]}
  "
  [{:keys [metadata-connector]} metadata topics conf]
  {:pre [metadata-connector
         (s/validate (s/either [s/Str] #{s/Str}) topics)
         (s/validate kafka-metadata/META-RESP-SCHEMA metadata)]}
  "Builds the datastructure {broker {topic [{:offset o :partition p} ...] }}"

  (let [topics-set (into #{} topics)
        offset-fn (fn [topic partition-info]
                    (let [
                          ;;produce {broker [[broker {:partition 0}] [broker {:partition 1}]]}
                          broker-partition-pairs (group-by first (map-indexed (fn [i host-info]
                                                                                [host-info {:partition i}]) partition-info))

                          ;;produce -> {broker {topic [{:offset offset :partition partition}]}} for the speficic topic
                          offset-maps (reduce-kv (fn [m broker broker-partition-pairs]
                                                   ;; broker-partition-pairs
                                                   ;; [
                                                   ;; [
                                                   ;;  {:host "localhost", :port 51718, :isr [{:host "localhost", :port 51718}], :id 0, :error-code 0} {:partition 0}
                                                   ;; ]
                                                   ;; ]
                                                   (if (.get (:closed metadata-connector))
                                                   m
                                                   (assoc m
                                                     broker
                                                     (get-offsets metadata-connector
                                                                  broker
                                                                  topic ;;produce [{:partition N} ...]
                                                                  (map second broker-partition-pairs)))))
                                                 {}
                                                 broker-partition-pairs)]
                      offset-maps))]
    (reduce-kv
      (fn [m topic partition-info]
        (if (topics-set topic)                              ;;filter out any topics not in the topics-set
          (merge-with
            merge
            m
            (offset-fn topic partition-info))
          m))
      {}
      metadata)))
