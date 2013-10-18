# clj-chunk-buffer

a write memory buffer base on chunk and TTL.

## Introduce
The chunk buffer usually can be used to output buffer for file system, storage or remote data writing. 

1. You can specify a chunk size threshold and a output function.
2. Then you can continuously write data to this buffer. The data will be buffered in memeory, I call it 'chunk'.
3. Specified output function will be invoked once chunk's size over the threshold. The function will be run in a separate daemon thread.
4. Output function also will be invoked once chunk expired specified TTL
5. You can cache many chunks in memory at the same time, you must give each chunk a unique key.

## Usage
Add the following dependency to your `project.clj` file:

    [clj-chunk-buffer "0.2.0"]

Create a buffer by `mk-chunk-buffer`:
```clojure
(ns for-test
  (:require [clj-chunk-buffer.core :as chunk-buffer]))

(def buffer (chunk-buffer/mk-chunk-buffer {:worker-num 5
                                           :chunk-size 1024 * 1024 ; 1M
                                           :chunk-age 60  ; 1 minutes
                                           :queue-limit 10 ; chunk queue size
                                           :worker-fn (fn [chunk] .... ) ; the output function 
                                                }))
```

To explain the options:

- chunk-size: the chunk's size limiation.
- chunk-age: The chunk's TTL
- queue-limit: Before expired or over sized chunk be processed by output function, it be stored in a queue in the underlying. You can specified the queue's maximum size. If chunk is over size and queue is full, writing to chunk will fail.
- worker-fn: the output funkction. We call it 'Worker' which consume the chunk queue.
- worker-num: how many threads to process the chunk in the some time.

Write data to buffer by `(write buffer data)`, and the data writed to buffer must extend protocol `ChunkData`

```clojure
;;; you must extend string ChunkData if you want to write string to buffer.
(extend-type String
  chunk-buffer/ChunkData
  (size [this] (count this)) ;;; buffer use this function to calculate chunk's total size.
  )
  
(chunk-buffer/write buffer "chunk-key" "this-is-data")
```


## License

Copyright © 2013 FIXME

Distributed under the Eclipse Public License, the same as Clojure.

