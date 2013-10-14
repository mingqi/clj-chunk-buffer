(ns clj-chunk-buffer.core-test
  (:require [clj-chunk-buffer.job :as job]
            [clojure.java.io :as io])
  (:use clojure.test
        clj-chunk-buffer.core)
  (:import [java.util.concurrent LinkedBlockingQueue]))

(deftest a-test
  (testing "FIXME, I fail."
    (is (= 0 0))))


(extend-type String
  ChunkData
  (size [this] (count this)))

(deftest test-chunk-age
  (let [chunk-seq (atom [])
        buffer (mk-chunk-buffer {:chunk-age 3
                                 :worker-fn (fn [chunk] (swap! chunk-seq conj chunk))})]
    (dotimes [_ 10] (write buffer "test-chunk-age-key" "data1"))
    (is (= 0 (count @chunk-seq)))
    (Thread/sleep 5000)
    (is (= 1 (count @chunk-seq)))
    (is (= 50 (:size (first @chunk-seq))))
    (close buffer)
    ))

(deftest test-chunk-size
  (let [chunk-seq (atom [])
        buffer (mk-chunk-buffer {:chunk-size 100
                                 :worker-fn (fn [chunk] (swap! chunk-seq conj chunk))})]
    (dotimes [_ 11] (write buffer "test-chunk-size-key" "123456789"))
    (Thread/sleep 100)
    (is (= 0 (count @chunk-seq)))
    (write buffer "test-chunk-size-key" "12")
    (Thread/sleep 100)
    (is (= 1 (count @chunk-seq)))
    (is (= 99 (:size (first @chunk-seq))))
    (close buffer)
    ))

(deftest test-queue-limit 
  (let [buffer (mk-chunk-buffer {:queue-limit 5
                                 :chunk-size 10
                                 :worker-num 1
                                 :worker-fn (fn [chunk]  (Thread/sleep 3000))})]
    (dotimes [_ 6] (write buffer "test-queue-limit-key" "1234567890") (Thread/sleep 500))
    (is (write buffer "test-queue-limit-key" "1234567890"))
    (is (not (write buffer "test-queue-limit-key" "123")) "queue should be full, can't accept more data")
    (close buffer)
    ))

(deftest test-close
  (let [chunk-seq (atom [])
        buffer (mk-chunk-buffer {:worker-fn (fn [chunk] (swap! chunk-seq conj chunk))})]
    (write buffer "key" "1234567890")
    (is (= 0 (count @chunk-seq)))
    (close buffer)
    (Thread/sleep 2)
    (is (= 1 (count @chunk-seq)) "pending chunk should be process after close")
    ))

(defn test-periodic-job []
  (let [job (job/new-periodic-job 1 (fn [] (println "run...")))]
    (job/start job)
    (Thread/sleep 10000)
    (println "stopping job")
    (job/stop job)
    (println "stopped job")
    ))

(defn test-consumer-job []
  (let [blocking-queue (LinkedBlockingQueue. 50)
        job (job/new-consumer-job 2 blocking-queue #(println "consumer #" % )) ]
    (job/start job)
    (dotimes [n 50] (.offer blocking-queue n))
    (Thread/sleep 10000)
    (println "stopping job")
    (job/stop job)
    (println "stopped job")
    (Thread/sleep 1000)
    (dotimes [n 50] (.offer blocking-queue n))
    ))

(defn test-concurrence []
  (println "start test concurrence")
  (let [chunk-sequence (atom 0)
        buffer (mk-chunk-buffer {:queue-limit 100
                                 :chunk-size (* 10 1024 1024)
                                 :worker-fn (fn [chunk]
                                              (println "trigger a worker")
                                              (with-open [o (io/output-stream (io/file (str "/var/tmp/chunk-buffer/chunk-" (swap! chunk-sequence inc))))]
                                                (doseq [data (:data-seq chunk)]
                                                  (.write o (.getBytes (str data "\n")))
                                                  (.flush o)
                                                  )))
                                 })]
    (dotimes [_ 10]
      (.start (Thread. (fn []
                         (dotimes [_ 100000]
                           (write buffer "key" "12345678901234567890123456789012345678901234567890"))
                         ))))
    (Thread/sleep 10000)
    (println "closing buffer")
    (close buffer)
    ))


(defn -main [ & args]
  ;(test-periodic-job)
  ;(test-consumer-job)
  (test-concurrence)
  )
