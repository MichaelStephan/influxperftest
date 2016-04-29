(ns influxtest.core
  (:require [org.httpkit.client :as http]
            [clojure.core.async :refer [chan put! close!]]
            [clojure.string :refer [join]]
            [taoensso.timbre :refer [infof warnf]])
  (:use [slingshot.slingshot :only [throw+]])
  (:gen-class))

(def influx-endoint "http://localhost:8086")
(def influx-query-endpoint (str influx-endoint "/query"))
(def influx-write-endpoint (str influx-endoint "/write"))

(defn rand-chars [cnt]
  (take cnt (repeatedly (fn [] (rand-nth "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")))))

(def rand-value (partial rand 100))

(defn rand-name [cnt]
  (clojure.string/join "" (rand-chars cnt)))

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

(defn rand-measurement-template [measurement-templates]
  (repeatedly (fn []
                (rand-nth measurement-templates))))

(defn measurement->line-protocol [[measurement {:keys [tags values]}]]
  (str (name measurement)
       (if (empty? tags) " " ",")
       (clojure.string/join "," (map (fn [[tag-name tag-values]]
                                       (str (name tag-name) "=" (rand-nth tag-values))) (into [] tags)))
       " "
       (clojure.string/join "," (map (fn [[value-name value]]
                                       (str (name value-name) "=" (value))) (into [] values)))))

(defn create-database [database]
  (let [{:keys [status error]} @(http/get influx-query-endpoint
                                          {:query-params {:q (str "create database " database)}})]
    (when (or error (not= status 200))
      (infof "Failed to create database %s - status: %s/ error: %s" database status error)
      (throw+ :create-database-error))))

(defn drop-database [database]
  (let [{:keys [status error]} @(http/get influx-query-endpoint
                                          {:query-params {:q (str "drop database " database)}})]
    (when (or error (not= status 200))
      (warnf "Failed to drop database %s - status: %s/ error: %s" database status error))))

(defn initialize-influx [{:keys [database]}]
  (create-database database))

(defn destroy-influx [{:keys [database]}]
  (drop-database database))

(defn write-measurements [database measurements] 
  (Thread/sleep 100)
  (let [ret (chan)
        start-time (System/nanoTime)
        measurements (clojure.string/join "\n" measurements)]
    (http/post influx-write-endpoint {:headers {"content-type" "application/x-www-form-urlencoded"}
                                      :query-params {:db database}
                                      :body measurements}
               (fn [{:keys [status error]}]
                 (let [end-time (System/nanoTime)
                       duration (- end-time start-time)]
                   (when (or error (not= status 204))
                     (warnf "Failed to write measurement '%s' to database %s - status: %s/ error: %s" measurements database status error))
                   (close! ret))))
    ret))

(def templates-config [[[3 3] 2]
                       [[] 1]
                       [[3 1] 5]])

(def templates (measurement-templates  {} templates-config))

; (inc (rand-int 100))

(defn -main [& args]
  (dotimes [n 100]
    #_(Thread/sleep 300)
    (->> templates
           rand-measurement-template
           (take 1)
           (map measurement->line-protocol)
           (write-measurements "testX"))))
  


