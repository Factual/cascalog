(ns cascalog.workflow
  (:refer-clojure
   :exclude [group-by count first filter mapcat map identity min max])
  (:use [cascalog.debug :only (debug-print)]
        [clojure.tools.macro :only (name-with-attributes)]
        [jackknife.core :only (safe-assert)]
        [jackknife.seq :only (collectify)])
  (:require [cascalog.conf :as conf]
            [cascalog.vars :as v]
            [cascalog.util :as u]
            [hadoop-util.core :as hadoop])
  (:import [cascalog Util]
           [java.io File]
           [java.util ArrayList]
           [cascading.tuple Tuple TupleEntry Fields]
           [cascading.scheme.hadoop TextLine SequenceFile TextDelimited]
           [cascading.scheme Scheme]
           [cascading.tap Tap SinkMode]
           [cascading.tap.hadoop Hfs Lfs GlobHfs TemplateTap]
           [cascading.tuple TupleEntryCollector]
           [cascading.flow Flow  FlowDef]
           [cascading.flow.hadoop HadoopFlowProcess HadoopFlowConnector]
           [cascading.cascade Cascades]
           [cascalog.ops KryoInsert]
           [cascading.operation Identity Debug]
           [cascading.operation.aggregator First Count Sum Min Max]
           [cascading.pipe Pipe Each Every GroupBy CoGroup]
           [cascading.pipe.joiner InnerJoin OuterJoin LeftJoin RightJoin MixedJoin]
           [com.twitter.maple.tap MemorySourceTap]
           [cascalog ClojureFilter ClojureMapcat ClojureMap
            ClojureAggregator Util ClojureBuffer ClojureBufferIter
            FastFirst MultiGroupBy ClojureMultibuffer]))

(def ^:dynamic *serializable-options* [:before-hook :after-hook])
(defn serializable-options [options] (select-keys options *serializable-options*))

(defn ns-fn-name-pair [v]
  (let [m (meta v)]
    [(str (:ns m)) (str (:name m))]))

(defn fn-spec [v-or-coll]
  "v-or-coll => var or [var & params]
   Returns an Object array that is used to represent a Clojure function.
   If the argument is a var, the array represents that function.
   If the argument is a coll, the array represents the function returned
   by applying the first element, which should be a var, to the rest of the
   elements."
  (cond
   (var? v-or-coll)
   (into-array Object (ns-fn-name-pair v-or-coll))

   (coll? v-or-coll)
   (into-array Object
               (concat
                (ns-fn-name-pair (clojure.core/first v-or-coll))
                (next v-or-coll)))

   :else (throw (IllegalArgumentException. (str v-or-coll)))))

(defn fields
  {:tag Fields}
  [obj]
  (if (or (nil? obj) (instance? Fields obj))
    obj
    (let [obj (collectify obj)]
      (if (empty? obj)
        Fields/ALL ; TODO: add Fields/NONE support
        (Fields. (into-array String obj))))))

(defn fields-array
  [fields-seq]
  (into-array Fields (clojure.core/map fields fields-seq)))

(defn pipes-array
  [pipes]
  (into-array Pipe pipes))

(defn- fields-obj? [obj]
  "Returns true for a Fields instance, a string, or an array of strings."
  (or
   (instance? Fields obj)
   (string? obj)
   (and (sequential? obj) (every? string? obj))))

(defn parse-args
  "arr => func-spec in-fields? :fn> func-fields :> out-fields

  returns [in-fields func-fields spec out-fields]"
  ([arr] (parse-args arr Fields/RESULTS))
  ([[func-args & varargs] defaultout]
     (let [spec      (fn-spec func-args)
           func-var  (if (var? func-args)
                       func-args
                       (clojure.core/first func-args))
           first-elem (clojure.core/first varargs)
           [in-fields keyargs] (if (or (nil? first-elem)
                                       (keyword? first-elem))
                                 [Fields/ALL (apply hash-map varargs)]
                                 [(fields (clojure.core/first varargs))
                                  (apply hash-map (rest varargs))])
           options  (merge {:fn> (:fields (meta func-var)) :> defaultout} keyargs)]
       [in-fields (fields (:fn> options)) spec (fields (:> options))])))

(defn pipe
  "Returns a Pipe of the given name, or if one is not supplied with a
   unique random name."
  ([] (pipe (u/uuid)))
  ([^String name]
     (Pipe. name)))

(defn pipe-append
  [^String s]
  (fn [p & [options]]
    (debug-print "pipe-append" s)
    (Pipe. (str (.getName p) s) p)))

(defn pipe-rename
  [^String name]
  (fn [p & [options]]
    (debug-print "pipe-rename" name)
    (Pipe. name p)))

(defn with-name [name x] (cond (instance? Pipe x) (Pipe. (str name) x)
                               (map? x)           (merge x {:pipe (with-name name (:pipe x))})
                               :else              x))

(defn name-of [x] (cond (instance? Pipe x) (.getName x)
                        (instance? Tap x)  "<tap>"
                        (map? x)           (name-of (:pipe x))
                        :else              (str x)))

(defn- as-pipes
  [pipe-or-pipes]
  (let [pipes (if (instance? Pipe pipe-or-pipes)
                [pipe-or-pipes] pipe-or-pipes)]
    (into-array Pipe (clojure.core/filter #(instance? Pipe %) pipes))))

;; with a :fn> defined, turns into a function
(defn filter [& args]
  (fn [previous & [options]]
    (debug-print "filter" args)
    (with-name (str (name-of previous) " -> filter " args)
      (let [[in-fields func-fields spec out-fields] (parse-args args)]
        (if func-fields
          (Each. previous in-fields
                 (ClojureMap. func-fields spec (serializable-options options)) out-fields)
          (Each. previous in-fields
                 (ClojureFilter. spec (serializable-options options))))))))

(defn mapcat [& args]
  (fn [previous & [options]]
    (debug-print "mapcat" args)
    (with-name (str (name-of previous) " -> mapcat " args)
      (let [[in-fields func-fields spec out-fields] (parse-args args)]
        (Each. previous in-fields
               (ClojureMapcat. func-fields spec (serializable-options options)) out-fields)))))

(defn map [& args]
  (fn [previous & [options]]
    (debug-print "map" args)
    (with-name (str (name-of previous) " -> map " args)
      (let [[in-fields func-fields spec out-fields] (parse-args args)]
        (Each. previous in-fields
               (ClojureMap. func-fields spec (serializable-options options)) out-fields)))))

(defn group-by
  ([]
     (fn [& previous]
       (debug-print "groupby no grouping fields")
       (with-name (str "group-by nothing " (clojure.core/mapv name-of previous))
         (GroupBy. (as-pipes previous)))))
  ([group-fields]
     (fn [& previous]
       (debug-print "groupby" group-fields)
       (with-name (str "group-by " group-fields (clojure.core/mapv name-of previous))
         (GroupBy. (as-pipes previous) (fields group-fields)))))
  ([group-fields sort-fields]
     (fn [& previous]
       (debug-print "groupby" group-fields sort-fields)
       (with-name (str "group-by " group-fields " order-by " sort-fields
                            (clojure.core/mapv name-of previous))
         (GroupBy. (as-pipes previous) (fields group-fields) (fields sort-fields)))))
  ([group-fields sort-fields reverse-order]
     (fn [& previous]
       (debug-print "groupby" group-fields sort-fields reverse-order)
       (with-name (str "group-by " group-fields " order-by " (if reverse-order "desc" "")
                            sort-fields
                            (clojure.core/mapv name-of previous))
         (GroupBy. (as-pipes previous) (fields group-fields) (fields sort-fields) reverse-order)))))

(defn count [^String count-field]
  (fn [previous & [options]]
    (debug-print "count" count-field)
    (with-name (str "count " count-field " (" (name-of previous) ")")
      (Every. previous (Count. (fields count-field))))))

(defn sum [^String in-fields ^String sum-fields]
  (fn [previous & [options]]
    (debug-print "sum" in-fields sum-fields)
    (with-name (str "sum " sum-fields " (" (name-of previous) ")")
      (Every. previous (fields in-fields) (Sum. (fields sum-fields))))))

(defn min [^String in-fields ^String min-fields]
  (fn [previous & [options]]
    (debug-print "min" in-fields min-fields)
    (with-name (str "min " min-fields " (" (name-of previous) ")")
      (Every. previous (fields in-fields) (Min. (fields min-fields))))))

(defn max [^String in-fields ^String max-fields]
  (fn [previous & [options]]
    (debug-print "max" in-fields max-fields)
    (with-name (str "max " max-fields " (" (name-of previous) ")")
      (Every. previous (fields in-fields) (Max. (fields max-fields))))))

(defn first []
  (fn [previous & [options]]
    (debug-print "first")
    (Every. previous (First.) Fields/RESULTS)))

(defn fast-first []
  (fn [previous & [options]]
    (debug-print "fast-first")
    (Every. previous (FastFirst.) Fields/RESULTS)))

(defn select [keep-fields]
  (fn [previous & [options]]
    (debug-print "select" keep-fields)
    (with-name (str (name-of previous) " : " keep-fields)
      (let [ret (Each. previous (fields keep-fields) (Identity.))]
        ret
        ))))

(defn identity [& args]
  (fn [previous & [options]]
    (debug-print "identity" args)
    (with-name (name-of previous)
      ;;  + is a hack. TODO: split up parse-args into parse-args and parse-selector-args
      (let [[in-fields func-fields _ out-fields _] (parse-args (cons #'+ args) Fields/RESULTS)
            id-func (if func-fields (Identity. func-fields) (Identity.))]
        (Each. previous in-fields id-func out-fields)))))

(defn pipe-name [name]
  (fn [p & [options]]
    (debug-print "pipe-name" name)
    (Pipe. name p)))

(defn insert [newfields vals]
  (fn [previous & [options]]
    (debug-print "insert" newfields vals)
    (with-name (name-of previous)
      (Each. previous (KryoInsert. (fields newfields)
                                   (into-array Object (collectify vals)))
             Fields/ALL))))

(defn raw-each
  ([arg1] (fn [p & [options]] (debug-print "raw-each" arg1) (Each. p arg1)))
  ([arg1 arg2] (fn [p & [options]] (debug-print "raw-each" arg1 arg2) (Each. p arg1 arg2)))
  ([arg1 arg2 arg3] (fn [p & [options]]
                      (debug-print "raw-each" arg1 arg2 arg3)
                      (Each. p arg1 arg2 arg3))))

(defn debug []
  (raw-each (Debug. true)))

(defn raw-every
  ([arg1] (fn [p & [options]] (debug-print "raw-every" arg1) (Every. p arg1)))
  ([arg1 arg2] (fn [p & [options]] (debug-print "raw-every" arg1 arg2) (Every. p arg1 arg2)))
  ([arg1 arg2 arg3] (fn [p & [options]]
                      (debug-print "raw-every" arg1 arg2 arg3)
                      (Every. p arg1 arg2 arg3))))

(defn aggregate [& args]
  (fn [^Pipe previous & [options]]
    (debug-print "aggregate" args)
    (with-name (str "aggregate " args " (" (name-of previous) ")")
      (let [[^Fields in-fields func-fields specs ^Fields out-fields]
            (parse-args args Fields/ALL)]
        (Every. previous in-fields
                (ClojureAggregator. func-fields specs (serializable-options options)) out-fields)))))

(defn buffer [& args]
  (fn [^Pipe previous & [options]]
    (debug-print "buffer" args)
    (with-name (str "buffer " args " (" (name-of previous) ")")
      (let [[^Fields in-fields func-fields specs ^Fields out-fields]
            (parse-args args Fields/ALL)]
        (Every. previous in-fields
                (ClojureBuffer. func-fields specs (serializable-options options)) out-fields)))))

(defn bufferiter [& args]
  (fn [^Pipe previous & [options]]
    (debug-print "bufferiter" args)
    (with-name (str "bufferiter " args " (" (name-of previous) ")")
      (let [[^Fields in-fields func-fields specs ^Fields out-fields] (parse-args args Fields/ALL)]
        (Every. previous in-fields
                (ClojureBufferIter. func-fields specs (serializable-options options)) out-fields)))))

(defn multibuffer [& args]
  (fn [pipes fields-sum]
    (debug-print "multibuffer" args)
    (with-name (str "multibuffer " args " " (clojure.core/mapv name-of pipes))
      (let [[group-fields func-fields specs _] (parse-args args Fields/ALL)]
        (MultiGroupBy.
         pipes
         group-fields
         fields-sum
         (ClojureMultibuffer. func-fields specs nil))))))

;; we shouldn't need a seq for fields (b/c we know how many pipes we have)
(defn co-group
  [fields-seq declared-fields joiner]
  (fn [& pipes-seq]
    (debug-print "cogroup" fields-seq declared-fields joiner)
    (with-name (str "cogroup " (clojure.core/mapv name-of pipes-seq))
      (CoGroup.
       (pipes-array pipes-seq)
       (fields-array fields-seq)
       (fields declared-fields)
       joiner))))

(defn mixed-joiner [bool-seq]
  (MixedJoin. (boolean-array bool-seq)))

(defn outer-joiner [] (OuterJoin.))

(defn- update-arglists
  "Scans the forms of a def* operation and adds an appropriate
  `:arglists` entry to the supplied `sym`'s metadata."
  [sym [form :as args]]
  (let [arglists (if (vector? form)
                   (list form)
                   (clojure.core/map clojure.core/first args))]
    (u/meta-conj sym {:arglists (list 'quote arglists)})))

(defn- update-fields
  "Examines the first item in a def* operation's forms. If the first
  form defines a sequence of Cascading output fields, these are added
  to the supplied `sym`'s metadata and dropped from the form
  sequence. Else, `sym` and `forms` are left unchanged.

  This function will no longer be necessary, if Cascalog deprecates
  the ability to name output fields before the dynamic argument
  vector."
  [sym [form :as forms]]
  (if (string? (clojure.core/first form))
    [(u/meta-conj sym {:fields form}) (rest forms)]
    [sym forms]))

(defn assert-nonvariadic [args]
  (safe-assert (not (some #{'&} args))
               (str "Defops currently don't support variadic arguments.\n"
                    "The following argument vector is invalid: " args)))

(defn- parse-defop-args
  "Accepts a def* type and the body of a def* operation binding,
  outfits the function var with all appropriate metadata, and returns
  a 3-tuple containing `[fname f-args args]`.

  * `fname`: the function var.
  * `f-args`: static variable declaration vector.
  * `args`: dynamic variable declaration vector."
  [type [spec & args]]
  (let [[fname f-args] (if (sequential? spec)
                         [(clojure.core/first spec) (second spec)]
                         [spec nil])
        [fname args] (->> [fname args]
                          (apply name-with-attributes)
                          (apply update-fields))
        fname (update-arglists fname args)
        fname (u/meta-conj fname {:pred-type (keyword (name type))
                                  :hof? (boolean f-args)})]
    (assert-nonvariadic f-args)
    [fname f-args args]))

(defn- defop-helper
  "Binding helper for cascalog def* ops. Function value is tagged with
   appropriate cascalog metadata; metadata can be accessed with `(meta
   op)`, rather than the previous `(op :meta)` requirement. This is so
   you can pass operations around and dynamically create flows."
  [type args]
  (let  [[fname func-args funcdef] (parse-defop-args type args)
         args-sym        (gensym "args")
         args-sym-all    (gensym "argsall")
         runner-name     (symbol (str fname "__"))
         func-form       (if func-args
                           `[(var ~runner-name) ~@func-args]
                           `(var ~runner-name))
         runner-body     (if func-args
                           `(~func-args (fn ~@funcdef))
                           funcdef)
         assembly-args   (if func-args
                           `[~func-args & ~args-sym]
                           `[ & ~args-sym])]
    `(do (defn ~runner-name
           ~(assoc (meta fname)
              :no-doc true
              :skip-wiki true)
           ~@runner-body)
         (def ~fname
           (with-meta
             (fn [& ~args-sym-all]
               (let [~assembly-args ~args-sym-all]
                 (apply ~type ~func-form ~args-sym)))
             ~(meta fname))))))

(defmacro defmapop
  {:arglists '([name doc-string? attr-map? [fn-args*] body])}
  [& args]
  (defop-helper 'cascalog.workflow/map args))

(defmacro defmapcatop
  {:arglists '([name doc-string? attr-map? [fn-args*] body])}
  [& args]
  (defop-helper 'cascalog.workflow/mapcat args))

(defmacro deffilterop
  {:arglists '([name doc-string? attr-map? [fn-args*] body])}
  [& args]
  (defop-helper 'cascalog.workflow/filter args))

(defmacro defaggregateop
  {:arglists '([name doc-string? attr-map? [fn-args*] body])}
  [& args]
  (defop-helper 'cascalog.workflow/aggregate args))

(defmacro defbufferop
  {:arglists '([name doc-string? attr-map? [fn-args*] body])}
  [& args]
  (defop-helper 'cascalog.workflow/buffer args))

(defmacro defmultibufferop
  {:arglists '([name doc-string? attr-map? [fn-args*] body])}
  [& args]
  (defop-helper 'cascalog.workflow/multibuffer args))

(defmacro defbufferiterop
  {:arglists '([name doc-string? attr-map? [fn-args*] body])}
  [& args]
  (defop-helper 'cascalog.workflow/bufferiter args))

(defn assemble
  ([x] x)
  ([x form] (apply form (collectify x)))
  ([x form & more] (apply assemble (assemble x form) more)))

(defn assemble-with-options [options]
  (fn self ([x] x)
           ([x form] (apply form (concat (collectify x) [options])))
           ([x form & more] (apply self (self x form) more))))

(defmacro assembly
  ([args return]
     `(assembly ~args [] ~return))
  ([args bindings return]
     (let [pipify (fn [forms] (if (or (not (sequential? forms))
                                      (vector? forms))
                                forms
                                (cons 'cascalog.workflow/assemble forms)))
           return (pipify return)
           bindings (vec (clojure.core/map #(%1 %2) (cycle [clojure.core/identity pipify]) bindings))]
       `(fn ~args
          (let ~bindings
            ~return)))))

(defmacro defassembly
  ([name args return]
     `(defassembly ~name ~args [] ~return))
  ([name args bindings return]
     `(def ~name (cascalog.workflow/assembly ~args ~bindings ~return))))

(defn join-assembly [fields-seq declared-fields joiner]
  (assembly [& pipes-seq]
            (pipes-seq (co-group fields-seq declared-fields joiner))))

(defn inner-join [fields-seq declared-fields]
  (join-assembly fields-seq declared-fields (InnerJoin.)))

(defn outer-join [fields-seq declared-fields]
  (join-assembly fields-seq declared-fields (OuterJoin.)))

(defn taps-map [pipes taps]
  (Cascades/tapsMap (into-array Pipe pipes) (into-array Tap taps)))

(defn flow-def
  [flow-name sourcemap sinkmap trapmap tails]
  (doto (FlowDef.)
    (.setName flow-name)
    (.addSources sourcemap)
    (.addSinks sinkmap)
    (.addTraps trapmap)
    (.addTails (into-array Pipe tails))))

(defn mk-flow [sources sinks assembly]
  (let [sources (collectify sources)
        sinks   (collectify sinks)
        source-pipes (clojure.core/map #(Pipe. (str "spipe" %2))
                                       sources
                                       (iterate inc 0))
        tail-pipes (clojure.core/map #(Pipe. (str "tpipe" %2) %1)
                                     (collectify (apply assembly source-pipes))
                                     (iterate inc 0))]
    (.connect (HadoopFlowConnector.)
              (taps-map source-pipes sources)
              (taps-map tail-pipes sinks)
              (into-array Pipe tail-pipes))))

(defn text-line
  ([]
     (TextLine.))
  ([field-names]
     (TextLine. (fields field-names) (fields field-names)))
  ([source-fields sink-fields]
     (TextLine. (fields source-fields) (fields sink-fields))))

(defn sequence-file [field-names]
  (SequenceFile. (fields field-names)))

(deffilterop equal [& objs]
  (apply = objs))

(defn compose-straight-assemblies [& all]
  (fn [input & [options]]
    (apply (assemble-with-options options) input all)))

(defn path
  {:tag String}
  [x]
  (if (string? x) x (.getAbsolutePath ^File x)))

(def valid-sinkmode? #{:keep :update :replace})

(defn- sink-mode [kwd]
  {:pre [(or (nil? kwd) (valid-sinkmode? kwd))]}
  (case kwd
    :keep    SinkMode/KEEP
    :update  SinkMode/UPDATE
    :replace SinkMode/REPLACE
    SinkMode/KEEP))

(defn set-sinkparts!
  "If `sinkparts` is truthy, returns the supplied cascading scheme
with the `sinkparts` field updated appropriately; else, acts as
identity.  identity."
  [^Scheme scheme sinkparts]
  (if sinkparts
    (doto scheme (.setNumSinkParts sinkparts))
    scheme))

(defn hfs
  ([scheme path-or-file]
     (Hfs. scheme (path path-or-file)))
  ([^Scheme scheme path-or-file sinkmode]
     (Hfs. scheme
           (path path-or-file)
           (sink-mode sinkmode))))

(defn lfs
  ([scheme path-or-file]
     (Lfs. scheme (path path-or-file)))
  ([^Scheme scheme path-or-file sinkmode]
     (Lfs. scheme
           (path path-or-file)
           (sink-mode sinkmode))))

(defn glob-hfs [^Scheme scheme path-or-file source-pattern]
  (GlobHfs. scheme (str (path path-or-file)
                        source-pattern)))

(defn template-tap
  ([^Hfs parent sink-template]
     (TemplateTap. parent sink-template))
  ([^Hfs parent sink-template templatefields]
     (TemplateTap. parent
                   sink-template
                   (fields templatefields))))

(defn write-dot [^Flow flow ^String path]
  (.writeDOT flow path))

(defn exec [^Flow flow]
  (.complete flow))

(defn fill-tap! [^Tap tap xs]
  (with-open [^TupleEntryCollector collector
              (-> (hadoop/job-conf (conf/project-conf))
                  (HadoopFlowProcess.)
                  (.openTapForWrite tap))]
    (doseq [item xs]
      (.add collector (Util/coerceToTuple item)))))

(defn memory-source-tap
  ([tuples] (memory-source-tap Fields/ALL tuples))
  ([fields-in tuples]
     (let [tuples (->> tuples
                       (clojure.core/map #(Util/coerceToTuple %))
                       (ArrayList.))]
       (MemorySourceTap. tuples (fields fields-in)))))
