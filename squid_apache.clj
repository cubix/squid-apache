(ns squid-apache
  (:use [clojure.contrib duck-streams command-line str-utils])
  (:import [java.util Date]
	   [java.text SimpleDateFormat DateFormat FieldPosition]))

(defn parse [line]
  (re-split #"\s+" line))

(defn parse-time [time-str]
  (let [[ms frac] (map #(Long/parseLong %)
		       (rest (re-matches #"^(\d+)\.(\d+)$" time-str)))]
    (+ (* 1000 ms) frac)))

(defn format-date [#^java.lang.String format-str 
		   #^java.util.Date date]
  (let [str-buf (new StringBuffer)
	dateFormat (new SimpleDateFormat format-str)
	pos (new FieldPosition 0)]
    (. (. dateFormat format date str-buf pos) toString)))

(def format-date-apache 
     (partial format-date "dd/MMM/yyyy:HH:mm:ss Z"))

(defn get-line [file-seq]
  (first (take 1 file-seq)))

(defn time-val [file-seq]
   (parse-time (first (parse (get-line file-seq)))))

(defn min-pos [seqs]
  (let [times (map time-val seqs)
	limit  (count seqs)]
    (letfn [(find-min [pos min-so-far]
		      (if (< pos limit)
			(if (< (nth times pos) (nth times min-so-far))
			  (find-min (inc pos) pos)
			  (find-min (inc pos) min-so-far))
			min-so-far))]
      (find-min 0 0))))

(defn update-seq [pos seqs]
  (let [[first-half second-half] (split-at pos seqs)]
     (filter (comp not empty?) 
	     (concat first-half 
		     (list (rest (nth seqs pos))) 
		     (drop 1 second-half)))))

(defn next-lowest [log-seqs]
  (if (not (empty? log-seqs))
    (let [next-pos (min-pos log-seqs)
	  next-val (first (nth log-seqs next-pos))
	  new-seqs (update-seq next-pos log-seqs)]
      (lazy-seq (cons next-val (next-lowest new-seqs))))))
   
(defn proc-log [log-out logs-in]
(println "log-out" log-out "logs-in:" logs-in)
  (let [log-stream (next-lowest (map read-lines logs-in))]
    (binding [*out* (writer log-out)]
      (doseq [log log-stream]
	(let [[ts tr a Ss Hs rm ru un Sh A mt]  (parse log)
	      [_ squid-action status] (re-matches #"(.+)/(\d{3})" Ss)
	      [_ unknown1 unknown2] (re-matches #"(.+)/(.+)" Sh)]
	  (println (format "%s %s %s [%s] \"%s %s HTTP/1.0\" %s %s \"%s\" \"%s\" %s:%s"
			   a "-" un  
			   (format-date-apache 
			    (new Date (parse-time ts))) 
			   rm ru status Hs "-" "-" squid-action unknown1)))))))

(with-command-line 
 *command-line-args*
 (str "squid-apache -- Merge Squid logs and convert to Apache format\n"
      "usage: squid-apache --out|-o <output-file> <squid-file1> [ <squid-file2> ... ]")
 [[out o "New log file to be created"]
  in]
  (proc-log out in))
