(ns influxtest.core
  (:require [org.httpkit.client :as http]
            [clojure.core.async :as a :refer [chan put! close! go go-loop >! <! <!! pipeline-async onto-chan]]
            [clojure.string :refer [join]]
            [taoensso.timbre :refer [infof warnf]]
            [influxtest.influxdb :as db :refer [write-measurements initialize-influx destroy-influx]]
            [metrics.core :refer [new-registry]]
            [metrics.histograms :refer [defhistogram histogram update! percentiles smallest largest std-dev mean]]
            [clojure.walk :refer [keywordize-keys]]
            [clojure.pprint :refer [pprint]])
  (:use [slingshot.slingshot :only [throw+]])
  (:gen-class))

(def reg (new-registry))

(defn rand-chars [cnt]
  (take cnt (repeatedly (fn [] (rand-nth "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")))))

(defn rand-value []
  (.nextDouble (java.util.concurrent.ThreadLocalRandom/current)))

(defn rand-name [cnt]
  (join "" (rand-chars cnt)))

(defn measurement-template-configs []
  (repeatedly (fn []
                (if (< (rand-int 101) 25)
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

(defn line-protocol-measurements [templates]
  (repeatedly (fn []
                (->> templates
                     (rand-nth)
                     (measurement-template->measurement)
                     (measurement->line-protocol-measurement)))))

; 60 teams
; a 10 apps/ team
; a 20 metrics/ app
(def templates-config (take (reduce * [60 10 20]) (measurement-template-configs)))
(def templates (measurement-templates  {} templates-config))

(defn rand-line-protocol-measurements-ch 
  ([templates batch-size]
   (rand-line-protocol-measurements-ch -1 templates batch-size))
  ([n templates batch-size]
    (let [ret-ch (chan)]
      (go-loop [n n]
               (if (or (>= n 0)
                       (= n -1))
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

(defn test-write-measurements [database n-times batch-size]
  (let [measurements-ch (rand-line-protocol-measurements-ch n-times templates batch-size)
        ret-ch (write-measurements measurements-ch database)
        start (System/nanoTime)
        req-ok-resp-time-histo-name (name (gensym "req-ok-resp-time-histo"))]
    (go-loop [req-total 0
              req-ok 0 req-ko 0
              req-ok-resp-time-histo (histogram reg req-ok-resp-time-histo-name)]
             (if-let [ret (<! ret-ch)]
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
                  :batch-size batch-size
                  :req-msec (/ req-total duration)
                  :req {:count req-total
                        :ok {:count req-ok
                              :min (smallest req-ok-resp-time-histo) 
                              :max (largest req-ok-resp-time-histo) 
                              :mean (mean req-ok-resp-time-histo)
                              :std-dev (std-dev req-ok-resp-time-histo) 
                              :percentiles (percentiles req-ok-resp-time-histo [0.25 0.50 0.75 0.95 0.99 0.999])}
                        :ko {:count req-ko}}})))))

(defn -main [& args]
  (let [{:keys [--database --database-endpoint --n-writers --n-times --batch-size] :or {--database-endpoint "http://localhost:8086"
                                                                                        --n-writers 1
                                                                                        --n-times 1000
                                                                                        --batch-size 1000}}
        (-> (apply hash-map args) keywordize-keys)
        --n-times (if (string? --n-times) (read-string --n-times) --n-times)
        --n-writers (if (string? --n-writers) (read-string --n-writers) --n-writers)
        --batch-size (if (string? --batch-size) (read-string --batch-size) --batch-size)] 
    (assert (> --n-times 0))
    (assert (and (> --n-writers 0) (<= --n-writers (* 2 (.availableProcessors (Runtime/getRuntime))))))
    (assert (> --batch-size 0))
    (assert (not (empty? --database)))
    (assert (not (empty? --database-endpoint)))

    (reset! db/influx-endoint --database-endpoint) 
    (destroy-influx {:database --database})
    (initialize-influx {:database --database})
    (let [in-ch (chan)
          ret-ch (chan)]
      (pipeline-async --n-writers ret-ch (fn [i ret-ch]
                                           (go
                                             (->> (test-write-measurements --database --n-times --batch-size) (<!) (>! ret-ch))
                                             (close! ret-ch))) in-ch)
      (onto-chan in-ch (range --n-writers))
      (loop [min-start (Long/MAX_VALUE) ok-count 0 ko-count 0]
        (if-let [ret (<!! ret-ch)]
          (let [{:keys [start]} ret]
            (pprint ret)
            (recur (if (< start min-start) start min-start)
                   (+ (get-in ret [:req :ok :count]) ok-count)
                   (+ (get-in ret [:req :ko :count]) ko-count)))
          (println {:req-msec (/ ok-count
                                 (nano->msec (- (System/nanoTime) min-start)))}))))
    (destroy-influx {:database --database})))



