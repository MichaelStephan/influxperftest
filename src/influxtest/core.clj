(ns influxtest.core
  (:require [org.httpkit.client :as http]
            [clojure.core.async :refer [chan put! close! go go-loop >! <! <!!]]
            [clojure.string :refer [join]]
            [taoensso.timbre :refer [infof warnf]]
            [influxtest.influxdb :refer [write-measurements]])
  (:use [slingshot.slingshot :only [throw+]])
  (:gen-class))

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
  ([templates]
   (rand-line-protocol-measurements-ch -1 templates))
  ([n templates]
    (let [ret-ch (chan)]
      (go-loop [n n]
               (if (or (> n 0)
                       (= n -1))
                 (do
                   (->> templates
                        (line-protocol-measurements)
                        (take 1000)
                        (>! ret-ch))
                   (recur (dec n)))
                 (close! ret-ch)))
      ret-ch)))

(defn nano->msec [v]
  (/ v 1000000.0))

(defn test-write-measurements []
  (let [measurements-ch (rand-line-protocol-measurements-ch 1000 templates)
        ret-ch (write-measurements measurements-ch "testX")
        start (System/nanoTime)]
    (go-loop [req-total 0
              req-ok 0 req-ko 0
              req-ok-resp-time 0 req-ok-resp-time-min (Long/MAX_VALUE) req-ok-resp-time-max 0]
             (if-let [ret (<! ret-ch)]
               (let [{:keys [count duration status error]} ret
                     success? (and (not error) (< status 400))]
                 (if success?
                   (recur (+ req-total count)
                          (+ req-ok count)
                          req-ko
                          (+ req-ok-resp-time duration)
                          (if (< duration req-ok-resp-time-min) duration req-ok-resp-time-min)
                          (if (> duration req-ok-resp-time-max) duration req-ok-resp-time-max))
                   (recur (+ req-total count)
                          req-ok
                          (+ req-ko count)
                          req-ok-resp-time
                          req-ok-resp-time-min
                          req-ok-resp-time-max)))
               (let [duration (nano->sec (- (System/nanoTime) start))]
                 {:duration duration 
                  :req-msec (/ req-total duration) 
                  :req {:count req-total
                        :ok {:count req-ok
                             :avg (/ req-ok-resp-time req-ok)
                             :min (nano->sec req-ok-resp-time-min)
                             :max (nano->sec req-ok-resp-time-max)}
                        :ko req-ko}})))))

(defn test []
  (go
    (let [x1 (test-write-measurements)
          x2 (test-write-measurements)
          x3 (test-write-measurements)]
      [(<! x1) (<! x2) (<! x3)])))


(defn -main [& args]) 

