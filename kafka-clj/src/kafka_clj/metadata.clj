(ns
  ^{:doc "All functions are concerned with retreiving metadata and offsets from kafka brokers
          part of this task requires blacklisting of brokers that do not response and retrying on connection errors"}
  kafka-clj.metadata
  (:require
    [clj-tuple :refer [tuple]]
    [kafka-clj.buff-utils :refer [write-short-string with-size compression-code-mask]]
    [kafka-clj.response :as kafka-resp]
    [clojure.tools.logging :refer [info error warn debug]]
    [kafka-clj.protocol :as protocol]
    [tcp-driver.driver :as tcp-driver]
    [tcp-driver.io.pool :as tcp-pool]
    [tcp-driver.io.conn :as tcp-conn]

    [tcp-driver.io.stream :as tcp-stream]

    [kafka-clj.tcp :as tcp]
    [tcp-driver.io.stream :as driver-io]
    [schema.core :as s])
  (:import
    (io.netty.buffer Unpooled ByteBuf)
    (kafka_clj.util Util TestUtils)
    (java.util.concurrent.atomic AtomicBoolean)
    (java.nio ByteBuffer)
    (org.apache.kafka.common.requests MetadataResponse MetadataResponse$TopicMetadata MetadataResponse$PartitionMetadata RequestHeader)
    (org.apache.kafka.clients NetworkClient)
    (org.apache.kafka.common.protocol Errors)
    (java.util HashSet Set ArrayList Collections List)))

;;validates metadata responses like {"abc" [{:host "localhost", :port 50738, :isr [{:host "localhost", :port 50738}], :id 0, :error-code 0}]}
(def META-RESP-SCHEMA {s/Str [{:host s/Str, :port s/Int, :isr [{:host s/Str, :port s/Int}], :id s/Int, :error-code s/Int}]})

(defonce ^List connectors (Collections/synchronizedList (ArrayList.)))

(defn- convert-metadata-response
  "
    transform the resp into a map
     {topic-name [{:host host :port port :isr [ {:host :port}] :error-code code} ] }
     the index of the vector (value of the topic-name) is sorted by partition number
     here topic-name:String and partition-n:Integer are keys but not keywords
    {:correlation-id 2,
                           :brokers [{:node-id 0, :host a, :port 9092}],
                           :topics
                           [{:error-code 10,
                             :topic p,
                             :partitions
                             [{:partition-error-code 10,
                               :partition-id 0,
                               :leader 0,
                               :replicas '(0 1),
                               :isr '(0)}]}]}

   "
    [resp]

    (let [m (let [;convert the brokers to a map {:broker-node-id {:host host :port port}}
                  brokers-by-node (into {} (map (fn [{:keys [node-id host port]}] [ node-id {:host host :port port}]) (:brokers resp)))]
                  ;convert the response message to a map {topic-name {partition {:host host :port port}}}
                  (into {}
                         (for [topic (:topics resp) :when (= (:error-code topic) 0)]
                              [(:topic topic) (apply tuple (vals (apply sorted-map (flatten
                                                                                      (for [partition (:partitions topic)
                                                                                             :let [broker (get brokers-by-node (:leader partition))
                                                                                                   isr (mapv #(get brokers-by-node %) (:isr partition))]]
                                                                                         [(:partition-id partition)
                                                                                          {:host (:host broker)
                                                                                           :port (:port broker)
                                                                                           :isr  isr
                                                                                           :id (:partition-id partition)
                                                                                           :error-code (:partition-error-code partition)}])))))])))]
      m))



(defn write-metadata-request
  "
   see http://kafka.apache.org/protocol.html
   RequestMessage => ApiKey ApiVersion CorrelationId ClientId RequestMessage
    ApiKey => int16
    ApiVersion => int16
    CorrelationId => int32
    ClientId => string
    MetadataRequest => [TopicName]
       TopicName => string
   "
  [^ByteBuf buff {:keys [correlation-id client-id] :or {correlation-id 1 client-id "1"}}]
  (-> buff
      (.writeShort (short protocol/API_KEY_METADATA_REQUEST))        ;api-key
      (.writeShort (short protocol/API_VERSION))                     ;version api
      (.writeInt (int correlation-id))                      ;correlation id
      (write-short-string client-id)                        ;short + client-id bytes
      (.writeInt (int 0))))                                 ;write empty topic, dont use -1 (this means nil), list to receive metadata on all topics

(defonce ^AtomicBoolean sucessParse (AtomicBoolean. false))

(defn send-recv-metadata-request
  "Writes out a metadata request to the producer con"
  [conn conf]
  (let [timeout 30000

        ^ByteBuf buff (Unpooled/buffer)
        _ (with-size buff write-metadata-request conf)

        ^"[B" arr (Util/toBytes buff)

        _  (driver-io/write-bytes conn arr 0 (count arr))

        _ (driver-io/flush-out conn)

        resp-len (driver-io/read-int conn timeout)

        resp-buff (ByteBuffer/wrap
                    ^"[B" (driver-io/read-bytes conn resp-len timeout))

        metadata (try
                   (MetadataResponse. (NetworkClient/parseResponse ^ByteBuffer resp-buff
                                                                   (RequestHeader. (short protocol/API_KEY_METADATA_REQUEST)
                                                                                   (short protocol/API_VERSION)
                                                                                   (get conf :client-id "1")
                                                                                   (get conf :correlation-id 1))))
                   (catch Exception exc (do
                                          (error exc ""))))]
        (if metadata
          (let [accept-topic (fn [^MetadataResponse$TopicMetadata topicMeta]
                               (let [^Errors error-obj (.error topicMeta)
                                     error-code (.code error-obj)]
                                 (if (zero? error-code)
                                   (not (.isInternal topicMeta))
                                   (error "Encountered topic error during metadata refresh, topic: "
                                          (.topic topicMeta) ", error: " (Util/errorToString error-obj) ", topic metadata discarded"))))

                accept-partition (fn [^MetadataResponse$TopicMetadata topicMeta ^MetadataResponse$PartitionMetadata partitionMeta]
                                   (let [^Errors error-obj (.error partitionMeta)
                                         error-code (.code error-obj)
                                         _ (when (not (zero? error-code)) (warn "Encountered partition error during metadata refresh, topic: "
                                                                                (.topic topicMeta) ", partition: " (.partition partitionMeta)
                                                                                ", error: " (Util/errorToString error-obj)))
                                         leader (.leader partitionMeta)]
                                     (and (not (nil? leader)) (not (empty? (.isr partitionMeta))))))
                hosts (HashSet.)
                converted-filtered-meta (Util/getMetaByTopicPartition metadata accept-topic accept-partition hosts)]
            [converted-filtered-meta hosts])
          [nil nil])))

  (defn safe-nth [coll i]
    (let [v (vec coll)]
      (when (> (count v) i)
        (nth v i))))

  (defn get-cached-metadata
    "from the metadata connector, reads the metadata in the metadata-ref

     Just calling metadata without a partition returns ;;[{:host \"localhost\", :port 50738, :isr [{:host \"localhost\", :port 50738}]
     specifying a partition returns the corresponding entry"
    ([metadata-connector topic partition]
      {:pre [(number? partition)]}
     (safe-nth (get-cached-metadata metadata-connector topic)
               partition))

    ([metadata-connector topic]
     {:pre [metadata-connector (or (string? topic) (keyword? topic))]}

     (let [metadata @(:metadata-ref metadata-connector)]

       (get metadata topic))))

  (defn get-metadata
    "
    Return metadata in the form ;;validates metadata responses like {\"abc\" [{:host \"localhost\", :port 50738, :isr [{:host \"localhost\", :port 50738}], :id 0, :error-code 0}]}\n
    "
    [{:keys [driver]} conf]
    (:pre [driver])
    (tcp-driver/send-f driver
                       (fn [conn]
                         (locking conn
                           (try
                             (send-recv-metadata-request conn conf)
                             (catch IndexOutOfBoundsException ie (do
                                                                   (warn "Error in reading medata from producer " (dissoc conn :error) " error: " ie)
                                                                   nil))
                             (catch Exception e (do
                                                  (error e "Error retreiving metadata ")
                                                  (throw e))))))
                       10000))

;;;;validates metadata responses like {"abc" [{:host "localhost", :port 50738, :isr [{:host "localhost", :port 50738}], :id 0, :error-code 0}]}

(defn update-metadata!
  "Updates the borkers-ref, metadata-ref and add/remove hosts as per the metadata from the driver
        if meta is nil, any updates are skipped
   "
  [{:keys [closed bootstrap-brokers brokers-ref metadata-ref] :as meta-connector} conf]
  (when (not (.get closed))
    (let [[meta ^Set hosts] (get-metadata meta-connector conf)]
      (when (not (nil? meta))
        (let [hosts-set (if (.isEmpty hosts) (set bootstrap-brokers) (set hosts))
              hosts-remove (clojure.set/difference @brokers-ref hosts-set)
              hosts-add (clojure.set/difference hosts-set @brokers-ref)]

          (doseq [connector connectors]
            (doseq [host hosts-add]
              (do (info "Adding host " host " to driver " (:driver connector))
                  (tcp-driver/add-host (:driver connector) host)))
            ;; to remove all brockers we need to get empty hosts metadata,
            ;; but if we get empty hosts metadata we will use bootstrap-brokers set
            (doseq [host hosts-remove]
              (do (info "Removing host " host " from driver " (:driver connector))
                  (tcp-driver/remove-host (:driver connector) host)))

            (dosync
              (commute
                (:brokers-ref connector) (constantly hosts-set))
              (commute
                (:metadata-ref connector) (constantly meta))))

          meta)))))

  (defn blacklisted? [{:keys [driver] :as st} host-address]
    {:pre [driver (:host host-address) (:port host-address)]}
    (tcp-driver/blacklisted? driver host-address))

  (defn connector
    "Creates a tcp pool and initiate for all brokers"
  [bootstrap-brokers conf]
  (let [driver (tcp/driver bootstrap-brokers :retry-limit (get conf :retry-limit 3) :pool-conf conf)
        brokers (into #{} bootstrap-brokers)
        result {:conf         conf
                :driver       driver
                :metadata-ref (ref nil)
                :bootstrap-brokers bootstrap-brokers
                :brokers-ref  (ref brokers)
                :closed       (AtomicBoolean. false)}
        _ (.add connectors result)]

    result))

(defn close [{:keys [driver closed]}]
  {:pre [driver closed]}
  (.set closed true)
  (tcp-driver/close driver))
