(ns influxtest.core
  (:require [org.httpkit.client :as http]
            [clojure.core.async :as a :refer [chan put! close! go go-loop >! <! <!! pipeline-async onto-chan timeout]]
            [clojure.string :refer [join]]
            [taoensso.timbre :refer [infof warnf]]
            [influxtest.influxdb :as db :refer [execute-query write-measurements initialize-influx destroy-influx]]
            [metrics.core :refer [new-registry]]
            [metrics.histograms :refer [defhistogram histogram update! percentiles smallest largest std-dev mean]]
            [clojure.walk :refer [keywordize-keys]]
            [clojure.pprint :refer [pprint]])
  (:use [slingshot.slingshot :only [throw+]])
  (:gen-class))

(def reg_ (new-registry))
(def continue_ (atom true))

(defn rand-chars [cnt]
  (take cnt (repeatedly (fn [] (rand-nth "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")))))

(defn rand-value []
  (.nextDouble (java.util.concurrent.ThreadLocalRandom/current)))

(defn rand-name [cnt]
  (join "" (rand-chars cnt)))

(defn measurement-template-configs []
  (repeatedly (fn []
                (if (< (rand-int 100) 30)
                  [[5 25] 1]
                  [[5] 0]))))

(defn measurement-templates [mms [[fields values] & rest]]
  (let [mm {(keyword (rand-name 20)) {:tags (or
                                             (apply merge (map (fn [field]
                                                                 {(keyword (rand-name 15)) (take field (repeatedly (fn [] (rand-name 10))))}) fields))
                                             [])
                                      :values (conj
                                               (repeatedly values (fn [] 
                                                                    [(keyword (rand-name 15)) rand-value]))
                                               [:value rand-value])}}
        mms (merge mms mm)]
    (if rest
      (recur mms rest)
      (into [] mms))))

(defn measurement-template->measurement [[measurement {:keys [tags values]}]]
  [measurement {:tags (apply merge (map (fn [[tag-name tag-values]]
                                          {tag-name (rand-nth tag-values)}) (into [] tags)))
                :values (apply merge (map (fn [[value-name value]]
                                            {value-name  (value)}) (into [] values)))}])

(defn measurement->line-protocol-measurement [[measurement {:keys [tags values]}]]
  (str (name measurement)
       (if (empty? tags) " " ",")
       (join "," (map (fn [[tag-name tag-value]]
                        (str (name tag-name) "=" tag-value)) (into [] tags)))
       " "
       (join "," (map (fn [[value-name value]]
                        (str (name value-name) "=" value)) (into [] values)))))

(def used-templates (atom #{}))
(defn line-protocol-measurements [templates]
  (repeatedly (fn []
                (let [template (->> templates (rand-nth))]
                  (swap! used-templates conj template)
                  (->> template
                       (measurement-template->measurement)
                       (measurement->line-protocol-measurement))))))

; 60 teams
; a 10 apps/ team
; a 20 metrics/ app
(def templates-config (take (* 60 10 20) (measurement-template-configs)))
(def templates (measurement-templates  {} templates-config))

(defn rand-line-protocol-measurements-ch 
  ([templates batch-size continue]
   (rand-line-protocol-measurements-ch -1 templates batch-size continue))
  ([n-times templates batch-size continue]
   (let [ret-ch (chan)]
     (go-loop [n n-times]
       (if (and @continue_
                (or (> n 0)
                    (<= n -1)))
         (do
           (->> templates
                (line-protocol-measurements)
                (take batch-size)
                (>! ret-ch))
           (recur (dec n)))
         (close! ret-ch)))
     ret-ch)))

(defn nano->msec [v]
  (/ v 1000000.0))

(defn run-test [test-ch]
  (let [req-ok-resp-time-histo-name (name (gensym "req-ok-resp-time-histo"))
        start (System/nanoTime)]
    (go-loop [req-total 0 req-ok 0 req-ko 0
              req-ok-resp-time-histo (histogram reg_ req-ok-resp-time-histo-name)]
      (if-let [ret (<! test-ch)]
        (let [{:keys [count duration status error]} ret
              success? (and (not error) (< status 400))]
          (if success?
            (do
              (update! req-ok-resp-time-histo (nano->msec duration))
              (recur (+ req-total count)
                     (+ req-ok count)
                     req-ko
                     req-ok-resp-time-histo))
            (recur (+ req-total count)
                   req-ok
                   (+ req-ko count)
                   req-ok-resp-time-histo)))
        (let [duration (nano->msec (- (System/nanoTime) start))]
          {:start start
           :duration duration 
           :req-msec (/ req-total duration)
           :req {:count req-total
                 :ok {:count req-ok
                      :min (smallest req-ok-resp-time-histo) 
                      :max (largest req-ok-resp-time-histo) 
                      :mean (mean req-ok-resp-time-histo)
                      :std-dev (std-dev req-ok-resp-time-histo) 
                      :percentiles (percentiles req-ok-resp-time-histo [0.25 0.50 0.75 0.95 0.99 0.999])}
                 :ko {:count req-ko}}})))))

(defn test-write-measurements [database n-times batch-size]
  (let [measurements-ch (rand-line-protocol-measurements-ch n-times templates batch-size continue_)]
    (run-test (write-measurements measurements-ch database))))

(defn select-query [[measurement {:keys [tags]}]]
  (let [tags? (not (empty? tags))
        selected (rand-int (inc (count tags)))]
    (str "select * from \"" (name measurement) "\" " (when (and tags? (> selected 0)) 
                                                       (str "where " (clojure.string/join " and " (take (inc selected) (map (fn [[k v]]
                                                                                                                              (str "\"" (name k) "\"='" (rand-nth v) "'")) tags)))))
         " limit " (+ (rand-int 5000) 500))))

(defn random-query [n-times quit]
  (let [ret-ch (chan)]
    (go-loop [i n-times] 
       (if (and @continue_
                (or (> n-times 0)
                    (<= n-times -1)))
        (do
          (>! ret-ch (select-query (let [template (take 1 (drop (rand-int (count @used-templates)) @used-templates))]
                                     (if (empty? template)
                                       (rand-nth templates)
                                       (first template)))))
          (recur (dec i)))
        (close! ret-ch)))
    ret-ch))

(defn test-read-query [database n-times]
  (run-test (execute-query (random-query n-times continue_) database)))

(defn run-parallel-test [name n-workers test-fn]
  (let [in-ch (chan) ret-ch (chan)]
    (pipeline-async n-workers ret-ch (fn [i ret-ch]
                                       (go
                                         (->> (test-fn) (<!) (>! ret-ch))
                                         (close! ret-ch))) in-ch)
    (onto-chan in-ch (range n-workers))
    (loop [min-start (Long/MAX_VALUE) ok-count 0 ko-count 0]
      (if-let [ret (<!! ret-ch)]
        (let [{:keys [start]} ret]
          (pprint (assoc ret :name name))
          (recur (if (< start min-start) start min-start)
                 (+ (get-in ret [:req :ok :count]) ok-count)
                 (+ (get-in ret [:req :ko :count]) ko-count)))
        (println {:name name
                  :req-msec (/ ok-count
                               (nano->msec (- (System/nanoTime) min-start)))})))))

(defn stop! []
  (reset! continue_ false))

(defn -main [& args]
  (let [available-processors (.availableProcessors (Runtime/getRuntime))
        {:keys [--database --database-endpoint --duration --n-writers --n-times-read --n-times-write --batch-size --n-readers] :or {--database-endpoint "http://localhost:8086"
                                                                                                                         --n-readers 1 
                                                                                                                         --n-writers 1
                                                                                                                         --duration 10 
                                                                                                                         --n-times-read -1 
                                                                                                                         --n-times-write -1 
                                                                                                                         --batch-size 1000}}
        (-> (apply hash-map args) keywordize-keys)
        --n-times-read (if (string? --n-times-read) (read-string --n-times-read) --n-times-read)
        --n-times-write (if (string? --n-times-write) (read-string --n-times-write) --n-times-write)
        --n-writers (if (string? --n-writers) (read-string --n-writers) --n-writers)
        --batch-size (if (string? --batch-size) (read-string --batch-size) --batch-size)
        --duration (if (string? --duration) (read-string --duration) --duration)] 
    (assert (and (>= --n-readers 0) (<= --n-readers (* 2 available-processors))))
    (assert (and (>= --n-writers 0) (<= --n-writers (* 2 available-processors))))
    (assert (> --batch-size 0))
    (assert (not (empty? --database)))
    (assert (not (empty? --database-endpoint)))

    (reset! continue_ true)
    (reset! db/influx-endoint --database-endpoint) 
    (doto {:database --database} destroy-influx initialize-influx)

    (when (not= -1 --duration) 
      (go-loop [start (System/currentTimeMillis)]
               (<! (timeout 1000))
               (if (> (- (System/currentTimeMillis) start) (* 1000 --duration))
                 (stop!)
                 (recur start))))

    (let [writes (future 
                   (if (> --n-writers 0)
                     (run-parallel-test "write measurement" --n-writers #(test-write-measurements --database --n-times-write --batch-size))
                     (println "skipping writes, writers set to zero")))
          reads (future
                  (if (> --n-readers 0)
                    (run-parallel-test "execute query" --n-readers #(test-read-query --database --n-times-read))
                    (println "skipping reads, readers set to zero")))]
      @writes
      (stop!)
      @reads)))


