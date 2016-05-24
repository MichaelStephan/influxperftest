(ns influxtest.influxdb
  (:require [org.httpkit.client :as http]
            [clojure.core.async :refer [chan put! close! go-loop >! <! <!!]]
            [clojure.string :refer [join]]
            [taoensso.timbre :refer [infof warnf]])
  (:use [slingshot.slingshot :only [throw+]]))

(def influx-endoint (atom "http://localhost:8086"))

(defn create-database [database]
  (let [{:keys [status error]} @(http/get (str @influx-endoint "/query")
                                          {:query-params {:q (str "create database " database)}})]
    (when (or error (not= status 200))
      (infof "Failed to create database %s - status: %s/ error: %s" database status error)
      (throw+ :create-database-error))))

(defn drop-database [database]
  (let [{:keys [status error]} @(http/get (str @influx-endoint "/query")
                                          {:query-params {:q (str "drop database " database)}})]
    (when (or error (not= status 200))
      (warnf "Failed to drop database %s - status: %s/ error: %s" database status error))))

(defn initialize-influx [{:keys [database]}]
  (create-database database))

(defn destroy-influx [{:keys [database]}]
  (drop-database database))

(defn execute-query [query-ch database] ; TODO bug
  (let [ret-ch (chan)]
    (go-loop []
      (if-let [query (<! query-ch)]
        (let [start-time (System/nanoTime)
              {:keys [status error body]} @(http/get (str @influx-endoint "/query") {:headers {"content-type" "application/x-www-form-urlencoded"}
                                                                                     :query-params {:db database
                                                                                                    :q query}})
              end-time (System/nanoTime)
              duration (- end-time start-time)]
          (put! ret-ch {:query query
                        :body body
                        :count 1
                        :duration duration
                        :status status
                        :error error})
          (recur))
        (close! ret-ch)))
    ret-ch))

(defn write-measurements [line-protocol-measurements-ch database] ; TODO bug
  (let [ret-ch (chan)]
    (go-loop []
      (if-let [line-protocol-measurements (<! line-protocol-measurements-ch)]
        (let [start-time (System/nanoTime)
              measurements (join "\n" line-protocol-measurements)
              {:keys [status error]} @(http/post (str @influx-endoint "/write") {:headers {"content-type" "application/x-www-form-urlencoded"}
                                                                                 :query-params {:db database}
                                                                                 :body measurements})
              end-time (System/nanoTime)
              duration (- end-time start-time)]

          (put! ret-ch {:count (count line-protocol-measurements) 
                        :duration duration
                        :status status
                        :error error})
          (recur))
        (close! ret-ch)))
    ret-ch))
