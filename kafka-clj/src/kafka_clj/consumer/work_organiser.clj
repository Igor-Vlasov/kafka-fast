(ns
  ^{:doc
    "
    Kafka divides topics into partitions and each partition starts at offset 0 till N.
    Standard clients take one partition to one client, but this creates coordination and scalalbility issues.
    E.g if a client died and did not remove its lock for a partition, a certian timeout needs to happen, but
    lets say the client is just slow and will recover eventually still thinking that it has a lock on the partition, when
    it might have timedout and start reading from this partition.

    Kafka-clj takes a different approach, it divides the partitions into work units each of length N.
    Then adds each work unit to a redis list.
    Each client will poll this redis list and take a work unit, then read the data for that work unit from kafka.
    If a work unit fails and put on the complete queue with a fail status, the work organiser will see this work unit
    and try to recalculate the metadata for it.

    This namespace requires a running redis and kafka cluster
     USAGE
    (use 'kafka-clj.consumer.work-organiser :reload)
    (def org (create-organiser!
    {:bootstrap-brokers [{:host \"localhost\" :port 9092}]
     :redis-conf {:host \"localhost\" :max-active 5 :timeout 1000} :working-queue \"working\" :complete-queue \"complete\" :work-queue \"work\" :conf {}}))
      (calculate-new-work org [\"ping\"])


    (use 'kafka-clj.consumer.consumer :reload)
    (def consumer (consumer-start {:redis-conf {:host \"localhost\" :max-active 5 :timeout 1000} :working-queue \"working\" :complete-queue \"complete\" :work-queue \"work\" :conf {}}))

    (def res (do-work-unit! consumer (fn [state status resp-data] state)))

     Basic data type is the work-unit
     structure is {:topic topic :partition partition :offset start-offset :len l :max-offset (+ start-offset l) :producer {:host host :port port}}

    "}
  kafka-clj.consumer.work-organiser
  (:require
    [kafka-clj.consumer.util :as cutil]
    [clojure.tools.logging :refer [info debug error warn]]
    [kafka-clj.redis.core :as redis]
    [kafka-clj.metadata :as kafka-metadata]
    [fun-utils.cache :as cache]
    [kafka-clj.consumer.workunits :refer [wait-on-work-unit!]]
    [schema.core :as s]
    [kafka-clj.schemas :as schemas])
  (:import [java.util.concurrent Executors ExecutorService CountDownLatch TimeUnit]
           (java.util.concurrent.atomic AtomicBoolean)
           (java.util HashMap Map Date)))


(declare get-offset-from-meta)
(declare close-organiser!)
(declare add-offsets!)
(declare calculate-work-units)

(defn- ^Long _to-int [s]
  (try
    (cond
      (integer? s) s
      (empty? s) -10
      :else (Long/parseLong (str s)))
    (catch NumberFormatException _ -10)))

(defn- ^Long to-int [s]
  (let [i (_to-int s)]
    (if (= i -10)
      (throw (RuntimeException. (str "Error reading number from s: " s)))
      i)))

(defn- work-complete-fail-delete!
  "
   Deletes the work unit by doing nothing i.e the work unit will not get pushed to the work queue again"
  [{:keys [redis-conn error-queue]} wu]
  (error "delete workunit error-code:1 " wu)
  (redis/lpush redis-conn error-queue (into (sorted-map) wu)))

(defn- work-complete-fail!
  "
   Tries to recalcualte the broker
   Side effects: Send data to redis work-queue"
  [{:keys [metadata-connector work-queue redis-conn conf] :as state} w-unit]
  (try
    ;we try to recalculate the broker, if any exception we reput the w-unit on the queue
    (let [sorted-wu (into (sorted-map) w-unit)
          broker (kafka-metadata/get-cached-metadata metadata-connector (:topic w-unit) (:partition w-unit))]
      (redis/lpush redis-conn work-queue (assoc sorted-wu :producer broker))
      state)
    (catch Exception e (do
                         (error e "")
                         (redis/lpush redis-conn work-queue (into (sorted-map) w-unit))))))

(defn- work-complete-ok!
  "If the work complete queue w-unit :status is ok
   If [diff = (offset + len) - read-offset] > 0 then the work is republished to the work-queue with offset == (inc read-offset) and len == diff

   Side effects: Send data to redis work-queue"
  [{:keys [work-queue redis-conn] :as state} {:keys [resp-data offset len] :as w-unit}]
  {:pre [work-queue resp-data offset len]}
  (let [sorted-wu (into (sorted-map) w-unit)
        offset-read (to-int (:offset-read resp-data))]
    (if (< offset-read (to-int offset))
      (do
        (info "Pushing zero read work-unit  " w-unit " sending to recalc")
        (work-complete-fail! state sorted-wu))
      (let [new-offset (inc offset-read)
            diff (- (+ (to-int offset) (to-int len)) new-offset)]
        (if (> diff 0)                                      ;if any offsets left, send work to work-queue with :offset = :offset-read :len diff
          (let [new-work-unit (assoc (dissoc sorted-wu :resp-data) :offset new-offset :len diff)]
            (info "Recalculating work for processed work-unit " new-work-unit " prev-wu " w-unit)
            (redis/lpush
              redis-conn
              work-queue
              new-work-unit)))))
    state))

(defn work-complete-handler!
  "If the status of the w-unit is :ok the work-unit is checked for remaining work, otherwise its completed, if :fail the work-unit is sent to the work-queue.
   Must be run inside a redis connection e.g car/wcar redis-conn"
  [state {:keys [status] :as w-unit}]
  (condp = status
    :fail (work-complete-fail! state w-unit)
    :fail-delete (work-complete-fail-delete! state w-unit)
    (work-complete-ok! state w-unit)))


(defn- ^Runnable work-complete-loop
  "Returns a Function that will loop continueously and wait for work on the complete queue"
  [{:keys [redis-conn complete-queue working-queue ^AtomicBoolean shutdown-flag ^CountDownLatch shutdown-confirm] :as state}]
  {:pre [redis-conn complete-queue working-queue]}
  (fn []
    (try
      (while (and (not (Thread/interrupted)) (not (.get shutdown-flag)))
        (try
          (when-let [work-units (wait-on-work-unit! redis-conn complete-queue working-queue shutdown-flag)]
            (redis/wcar
              redis-conn
              (if (map? work-units)
                (do
                  (work-complete-handler! state work-units)
                  (redis/lrem redis-conn working-queue -1 (into (sorted-map) work-units)))
                (doseq [work-unit work-units]
                  (work-complete-handler! state work-unit)
                  (redis/lrem redis-conn working-queue -1 (into (sorted-map) work-unit))))))
          (catch InterruptedException _ (info "Exit work complete loop"))
          (catch Exception e (error e ""))))
      (finally
        (.countDown shutdown-confirm)))))

(defn start-work-complete-processor!
  "Creates a ExecutorService and starts the work-complete-loop running in a background thread
   Returns the ExecutorService"
  [state]
  (doto (Executors/newSingleThreadExecutor)
    (.submit (work-complete-loop state))))

(defn calculate-work-units
  "Returns '({:topic :partition :offset :len :max-offset}) Len is exclusive"
  [producer topic partition ^Long max-offset ^Long start-offset ^Long step]
  {:pre [(and (:host producer) (:port producer))]}

  (if (< start-offset max-offset)
    (let [^Long t (+ start-offset step)
          ^Long l (if (> t max-offset) (- max-offset start-offset) step)]
      (cons
        {:topic topic :partition partition :offset start-offset :len l :max-offset (+ start-offset l)}
        (lazy-seq
          (calculate-work-units producer topic partition max-offset (+ start-offset l) step))))))

(defn check-offset-in-range
  "Check that the saved-offset still exists on the brokers and if not
   get the earliest offset and start from there"
  [state min-kafka-offset topic partition saved-offset]

  (let [earliest-offset  min-kafka-offset]

    (if (> earliest-offset saved-offset)
      (do
        (warn "for " topic ":" partition " offset saved " saved-offset " does not exist and the earliest offset available is " earliest-offset)
        earliest-offset)
      saved-offset)))

(defn ^Long get-saved-offset
  "Returns the last saved offset for a topic partition combination
   If the partition is not found for what ever reason the latest offset will be taken from the kafka meta data
   this value is saved to redis and then returned"
  [{:keys [group-name redis-conn] :as state} [min-kafka-offset max-kafka-offset] topic partition]
  {:pre [group-name redis-conn (:metadata-connector state) topic partition]}

  (let [offset (io!
                 (let [saved-offset (redis/wcar redis-conn (redis/get redis-conn (str "/" group-name "/offsets/" topic "/" partition)))]
                   (cond
                     (not (nil? saved-offset)) (check-offset-in-range state min-kafka-offset topic partition (to-int saved-offset))
                     :else (let [meta-offset (if (get-in state [:conf :use-earliest]) min-kafka-offset max-kafka-offset)]

                             (info "Set initial offsets [" topic "/" partition "]: " meta-offset)
                             (redis/wcar redis-conn (redis/set redis-conn (str "/" group-name "/offsets/" topic "/" partition) meta-offset))
                             meta-offset))))]
    offset))

(defn add-offsets! [state topic min-max-kafka-offsets offset-datum]
  {:pre [(s/validate [s/Int] min-max-kafka-offsets)
         (s/validate schemas/PARTITION-OFFSET-DATA offset-datum)]}
  (try
    (assoc offset-datum :saved-offset (get-saved-offset state min-max-kafka-offsets topic (:partition offset-datum)))
    (catch Throwable t (do (error t "") nil))))

(defn max-value
  "Nil safe max function"
  [& vals]
  (let [vals2 (filter #(not (nil? %)) vals)]
    (if (empty? vals2)
      0
      (apply max vals2))))

(defn send-offsets-if-any!
  "
  Calculates the work-units for saved-offset -> offset and persist to redis.
  Side effects: lpush work-units to work-queue
                set offsets/$topic/$partition = max-offset of work-units
   "
  [{:keys [group-name redis-conn work-queue ^Long consume-step work-assigned-flag conf] :as state} broker topic offset-data]
  {:pre [group-name redis-conn work-queue]}
  (let [offset-data2
        ;here we add offsets as saved-offset via the add-offset function
        ;note that this function will also write to redis
        (filter (fn [x] (and x (:saved-offset x) (:offset x) (< ^Long (:saved-offset x) ^Long (:offset x))))
                (map #(do
                       (let [all-offsets (:all-offsets %)
                             offset (:offset %)]
                         (if (and (empty? all-offsets) (nil? offset))
                           %
                          (add-offsets! state topic [(apply (fnil min 0) all-offsets) offset] %)))) offset-data))

        consume-step2 (if consume-step consume-step (get conf :consume-step 100000))]

    (doseq [{:keys [offset partition saved-offset all-offsets]} offset-data2]
      (swap! work-assigned-flag inc)

      ;;avoid calculating hundreds of thousands of workunits potentially at a time, limit to 10K
      (when-let [work-units (take 10000
                                  (calculate-work-units broker topic partition offset saved-offset consume-step2))]
        (let [max-offset (apply max (map #(+ ^Long (:offset %) ^Long (:len %)) work-units))
              ts (System/currentTimeMillis)]

          (redis/wcar redis-conn
                      ;we must use sorted-map here otherwise removing the wu will not be possible due to serialization with arbritary order of keys
                      (redis/lpush* redis-conn work-queue (map #(assoc (into (sorted-map) %) :producer broker :ts ts) work-units))
                      (redis/set redis-conn (str "/" group-name "/offsets/" topic "/" partition) max-offset)))))))

;(defn get-offset-from-meta [{:keys [metadata-connector conf]} topic partition]
;  {:pre [metadata-connector conf (string? topic) (number? partition)]}
;
;  (let [partition-offsets (:all-offsets (kafka-metadata/get-cached-metadata metadata-connector topic partition))]
;
;    (if (empty? partition-offsets)
;      (throw (ex-info "No offset data found" {:topic topic :partition partition :offsets partition-offsets})))
;
;    (debug "get-offset-from-meta: conf: " (keys conf) " use-earliest: " (:use-earliest conf) " partition-offsets " partition-offsets)
;
;    (if (= true (:use-earliest conf)) (apply min partition-offsets) (apply max partition-offsets))))

(defn- topic-partition? [m topic partition] (filter (fn [[t partition-data]] (and (= topic t) (not-empty (filter #(= (:partition %) partition) partition-data)))) m))

(defn _max-offset
  "
  Return the max save-offsetN and max of max-offsetN
  [saved-offset max-offset]
  [saved-offset2 max-offset2]

  returns [saved-offsetN max-offsetN]"
  [[saved-offset max-offset] [saved-offset2 max-offset2 :as v]]
  (if max-offset
    [(max saved-offset saved-offset2) (max max-offset max-offset2)]
    v))

(defn redis-offset-save [redis-conn group-name topic partition offset]
  (redis/wcar redis-conn
              (redis/set redis-conn (str "/" group-name "/offsets/" topic "/" partition) offset)))

(defn check-invalid-offsets!
  "Check for https://github.com/gerritjvv/kafka-fast/issues/10
   if a saved-offset > max-offset the redis offset is reset
   optional args
      redis-f (fn [redis-conn group-name topic partition offset] ) used to save the offset, the default impl is used to save to redis
      get-saved-offset (fn [state max-kafka-offset topic partition] ) get the last saved offset, default impl is get-saved-offset

      stats-atom (atom (map usefullstats))"
  [{:keys [group-name redis-conn stats-atom] :as state} read-ahead-offsets offsets & {:keys [redis-f saved-offset-f] :or {redis-f redis-offset-save saved-offset-f get-saved-offset}}]
  (let [^Map m (HashMap.)]

    ;;local mutable code to simplify getting max offsets for a topic partition that may be given
    ;;conflicting data by multiple brokers
    (doseq [[_ topic-data] offsets]
      (doseq [[topic offset-data] topic-data]

        (doseq [{:keys [partition all-offsets]} offset-data]
          (let [k {:topic topic :partition partition}
                empty-offsets (empty? all-offsets)
                max-kafka-offset (if empty-offsets 0 (apply (fnil max 0) all-offsets))
                min-kafka-offset (if empty-offsets 0 (apply (fnil min 0) all-offsets))]

            (.put m k (_max-offset (.get m k) [(saved-offset-f state
                                                               [min-kafka-offset max-kafka-offset]
                                                               topic
                                                               partition)
                                               max-kafka-offset]))))))

    (doseq [[{:keys [topic partition]} [saved-offset max-offset]] m]
      (when (> saved-offset max-offset)

        (swap! stats-atom update-in [:offsets-ahead-stats topic partition] (constantly {:saved-offset saved-offset :max-offset max-offset :date (Date.)}))

        (when read-ahead-offsets
          (redis-f redis-conn group-name topic partition max-offset))))))

(defn calculate-new-work
  "Accepts the state and returns the state as is.
   For topics new work is calculated depending on the metadata returned from the producers

   stats is an atom that contains a map of adhoc stats which might be of interest to the user"
  [{:keys [metadata-connector conf error-handler stats-atom] :as state} topics]
  {:pre [metadata-connector conf stats-atom]}
  (try

    (let [meta @(:metadata-ref metadata-connector)

          offsets (cutil/get-broker-offsets state meta topics conf)
          offset-send-f (fn [broker topic offset-data]
                          (try
                            ;we map :offset to max of :offset and :all-offets
                            (send-offsets-if-any! state broker topic (map #(assoc % :offset (apply max (:offset %) (:all-offsets %))) offset-data))
                            (catch Exception e (do (error e "") (if error-handler (error-handler :meta state e))))))]


      ;; meta
      ;;{
      ;; {:host "localhost", :port 52031, :isr [{:host "localhost", :port 52031}], :id 0, :error-code 0}
      ;; {"1487198655207"
      ;;  ({:offset 0, :all-offsets (10000 0), :error-code 0, :locked false, :partition 0})}}


      ;;offsets
      ;;{
      ;; {:host "localhost", :port 52031, :isr [{:host "localhost", :port 52031}], :id 0, :error-code 0}
      ;; {"1487198655207"
      ;;   ({:offset 0, :all-offsets (10000 0), :error-code 0, :locked false, :partition 0})
      ;; }
      ;;}


      ;;check for inconsistent offsets https://github.com/gerritjvv/kafka-fast/issues/10
      (check-invalid-offsets! state (get conf :reset-ahead-offsets false) offsets)

      (reduce-kv (fn [_ broker topic-data]
                   (reduce-kv (fn [_ topic offset-data]
                                (offset-send-f broker topic offset-data)) nil topic-data)) nil offsets)


      state)
    (catch Exception e (do
                         (error e "")
                         state))))

(defn create-organiser!
  "Create a organiser state that should be passed to all the functions were state is required in this namespace"
  [{:keys [bootstrap-brokers work-queue working-queue complete-queue redis-conf redis-factory conf] :or {redis-factory redis/create} :as state}]
  {:pre [work-queue working-queue complete-queue
         bootstrap-brokers (> (count bootstrap-brokers) 0) (-> bootstrap-brokers first :host) (-> bootstrap-brokers first :port)]}

  (let [shutdown-flag (AtomicBoolean. false)
        shutdown-confirm (CountDownLatch. 1)

        metadata-connector (kafka-metadata/connector bootstrap-brokers conf)

        meta (kafka-metadata/update-metadata! metadata-connector conf)

        redis-conn (redis-factory redis-conf)
        intermediate-state (assoc state :metadata-connector metadata-connector

                                        :redis-conn redis-conn
                                        :offset-producers (ref {})
                                        :shutdown-flag shutdown-flag
                                        :shutdown-confirm shutdown-confirm)

        work-complete-processor-future (start-work-complete-processor! intermediate-state)

        state (assoc intermediate-state
                :work-assigned-flag (atom 0)
                :work-complete-processor-future
                work-complete-processor-future)]

    ;;if no metadata throw an exception and close the organiser
    (when (nil? meta)
      (try
        (close-organiser! state)
        (catch Exception e (error e "")))
      (throw (ex-info (str "No metadata could be found from any of the bootstrap brokers provided " bootstrap-brokers) {:type :metadata-exception
                                                                                                                        :bootstrap-brokers bootstrap-brokers})))
    state))


(defn wait-on-work-assigned-flag [{:keys [work-assigned-flag]} timeout-ms]
  (loop [ts (System/currentTimeMillis)]
    (if (> @work-assigned-flag 0)
      @work-assigned-flag
      (if (>= (- (System/currentTimeMillis) ts) timeout-ms)
        -1
        (do
          (Thread/sleep 1000)

          (recur (System/currentTimeMillis)))))))

(defn close-organiser!
  "Closes the organiser passed in"
  [{:keys [metadata-connector
           work-complete-processor-future
           redis-conn ^AtomicBoolean shutdown-flag ^CountDownLatch shutdown-confirm]} & {:keys [close-redis] :or {close-redis true}}]
  {:pre [metadata-connector
         redis-conn
         (instance? ExecutorService work-complete-processor-future)]}
  (.set shutdown-flag true)
  (.await shutdown-confirm 10000 TimeUnit/MILLISECONDS)
  (.shutdown ^ExecutorService work-complete-processor-future)

  (kafka-metadata/close metadata-connector)

  (when close-redis
    (redis/close! redis-conn)))
