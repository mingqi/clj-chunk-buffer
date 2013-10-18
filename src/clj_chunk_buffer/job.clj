(ns clj-chunk-buffer.job
  (:require [taoensso.timbre :as log]
            [clojure.java.io :as io] 
            )   
  (:import [java.util.concurrent BlockingQueue LinkedBlockingQueue]
           [java.util.concurrent Executors TimeUnit Callable]
           ))

(defprotocol Job
  (start [job])
  (stop [job]))

(defn new-consumer-job [worker-num blocking-queue worker-fn]
  (let [termination-signal (atom false)] 
    (reify Job
      (start [job]
        (dotimes [_ worker-num]
          (.start (Thread.
                   #(while (not @termination-signal) 
                      (try
                        (if-let [chunk (. blocking-queue (poll 1 TimeUnit/SECONDS))]
                          (worker-fn chunk))
                        (catch Exception e)))))))
      
      (stop [job]
        (reset! termination-signal true))

      )))

(defn new-periodic-job [interval-sec, run-fn]
  (let [_future (atom nil)
        executor (atom nil)]
    (reify Job
      
      (start [job]
        (let [es (Executors/newSingleThreadScheduledExecutor)]
          (reset! _future
                  (.scheduleWithFixedDelay
                   es
                   run-fn
                   0,
                   interval-sec
                   TimeUnit/SECONDS))
          (reset! executor es)))

      (stop [job]
        (.cancel @_future true)
        (.shutdown @executor))
      )))
