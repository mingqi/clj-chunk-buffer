(ns clj-chunk-buffer.core
  (:require [taoensso.timbre :as log]
            [clj-chunk-buffer.job :as job])
  (:import [java.util.concurrent BlockingQueue LinkedBlockingQueue]))


;;;;;; the chunk ;;;;;;
(defprotocol ChunkData
  "all data store in chunk must be implement this interface"
  (size [this] "the size of data in chunk"))

(defrecord Chunk [key, created-epoch-sec, data-seq, size])

(defn- epoch []
  (int (/ (System/currentTimeMillis) 1000)))


(defn- mk-chunk [key]
  (->Chunk key, (epoch) [] 0))

(defn- push-chunk [chunk data]
  (->Chunk (:key chunk)
           (:created-epoch-sec chunk)
           (conj (:data-seq chunk) data)
           (+ (:size chunk) (size data))))

;;;;;; Buffer ;;;;;;
(defprotocol ChunkBuffer
  (write [this key data] "put data into buffer")
  (stat [this])
  (close [this] "stop buffer, stop accept data and force Workers to clean up all pending chunks"))

(defn mk-chunk-buffer
  "make a new chunk buffer"
  ([] (mk-chunk-buffer {}))
  ([opts]
     (let [opts (merge {:worker-num 1,
                        :chunk-size 1024 * 1024  ; 1M,
                        :queue-limit 5,
                        :chunk-age 60,
                        :worker-fn nil,
                        } opts)
           chunks-map (atom {})
           chunks-queue (LinkedBlockingQueue. (:queue-limit opts))
           worker-job (atom nil)
           collector-job (atom nil)]
       
       (letfn [(collect-chunk! [key]
                 (log/debug "trying to collect chunk, key is" key ", queue size is" (.size chunks-queue))
                 (let [chunk (@chunks-map key)]
                         (when (and chunk
                                    (> (:size chunk) 0))
                           (if (.offer chunks-queue chunk)
                             (do
                               (swap! chunks-map dissoc key)
                               chunk)
                             (log/warn "buffer queue is full, can't collect chunk, key is" key))
                           )))

               (write-chunk! [key data]
                 (swap! chunks-map update-in [key] #(push-chunk (or % (mk-chunk key)) data))
                 (@chunks-map key))

               (collect-old-chunks! []
                 (log/info "buffer stat: " (buffer-stat)) 
                 (doseq [chunk (vals @chunks-map)]
                   (when (>= (- (epoch) (:created-epoch-sec chunk)) (:chunk-age opts))
                     (collect-chunk! (:key chunk)))))

               (buffer-stat []
                 {:map {:count (count @chunks-map)
                        :size (reduce #(+ %1 (:size %2)) 0 (vals @chunks-map))},
                  :queue {:length (.size chunks-queue)
                          :size (reduce  #(+ %1 (:size %2)) 0 (seq chunks-queue))},
                  }
                 )
               ]

         ;;; start daemon jobs
         (job/start (reset! worker-job
                            (job/new-consumer-job
                             (:worker-num opts)
                             chunks-queue
                             (:worker-fn opts))))

         (job/start (reset! collector-job
                            (job/new-periodic-job 1 collect-old-chunks!)))

         (reify ChunkBuffer
           (stat [this]
              (buffer-stat))

           (write [this key data]
             (locking this
               (let [chunk (or (@chunks-map key) (mk-chunk key))]
                 (if (and (> (+ (size data) (:size chunk)) (:chunk-size opts))
                          (not (collect-chunk! key)))
                   (do
                     (log/warn "failed to collect chunk, discard data, key:" key ", size: " (size data))
                     false)
                   (write-chunk! key data)
                   ))))

           (close [this]
             (dotimes [_ 5]
               (when (> (count @chunks-map) 0)
                 (doseq [[key chunk] @chunks-map]
                   (collect-chunk! key))
                 (Thread/sleep 1000)))
             (job/stop @worker-job)
             (job/stop @collector-job))
           
           )))))


(defprotocol ProtoTest
  (f [this]))

(defrecord Person [name]
  ProtoTest
  (f [this] (println "this is Person ProtoTest")))

(defn ff []
  (let [x 1]
    (reify ProtoTest
      (f [this]
        (println "&& reify && " this)))))
