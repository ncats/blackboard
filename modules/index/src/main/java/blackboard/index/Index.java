package blackboard.index;

import play.Logger;
import play.libs.Json;

import javax.inject.Inject;
import java.io.*;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.*;
import java.util.zip.GZIPOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.concurrent.atomic.AtomicInteger;
import java.time.temporal.ChronoField;

import org.apache.lucene.store.*;
import org.apache.lucene.analysis.*;
import org.apache.lucene.index.*;
import org.apache.lucene.document.*;
import org.apache.lucene.facet.*;
import org.apache.lucene.facet.taxonomy.directory.*;
import org.apache.lucene.facet.taxonomy.*;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.search.*;
import org.apache.lucene.util.QueryBuilder;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.vectorhighlight.FastVectorHighlighter;
import org.apache.lucene.search.vectorhighlight.FieldQuery;
import org.apache.lucene.search.suggest.document.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonAnyGetter;

import javax.xml.parsers.DocumentBuilderFactory;
import com.google.inject.assistedinject.Assisted;

import blackboard.utils.Util;

public class Index implements AutoCloseable, Fields {
    public static final int MAX_HITS = 1000;
    
    public static class FV {
        @JsonIgnore
        public FV parent;
        public Integer total;
        public String display;
        public final String label;
        public boolean specified;
        public Integer count;
        public final List<FV> children = new ArrayList<>();

        protected FV (String label, Integer count) {
            this (label, count, false);
        }
        protected FV (String label, Integer count, boolean specified) {
            this.label = label;
            this.count = count;
            this.specified = specified;
        }

        public FV add (FV node) {
            node.parent = this;
            children.add(node);
            return this;
        }

        public boolean remove () {
            if (parent != null)
                return parent.children.remove(this);
            return false;
        }

        public String name () {
            if (display != null) return display;
            return label;
        }
        
        public String[] toPath () {
            List<String> path = new ArrayList<>();
            for (FV p = this; p != null; p = p.parent) {
                path.add(0, p.label);
            }
            return path.toArray(new String[0]);
        }

        public String toPath (String sep) {
            StringBuilder sb = new StringBuilder ();
            for (FV p = this; p != null; ) {
                FV parent = p.parent;
                sb.insert(0, parent != null ? sep+p.label : p.label);
                p = parent;
            }
            return sb.toString();
        }
        
        public String getPath () {
            return parent != null ? toPath (".") : label;
        }

        static void print (StringBuilder sb, FV fv) {
            for (FV p = fv; p != null; p = p.parent)
                sb.append(".");
            if (fv.display != null)
                sb.append("["+fv.label+"] "+fv.display+" ("+fv.count+")\n");
            else
                sb.append(fv.label+" ("+fv.count+")\n");
            for (FV child : fv.children)
                print (sb, child);
        }
        
        public String toTreeString () {
            StringBuilder sb = new StringBuilder ();
            print (sb, this);
            return sb.toString();
        }
        
        public String toString () { return getPath (); }
    }
    
    public static class Facet {
        public String display;
        public final String name;
        public final List<FV> values = new ArrayList<>();

        protected Facet (String name) {
            this.name = name;
        }

        public String toString () {
            StringBuilder sb = new StringBuilder ();
            sb.append("["+name+"]\n");
            for (FV fv : values)
                FV.print(sb, fv);
            return sb.toString();
        }

        public boolean hasValue (String value) {
            return null != getValue (value);
        }

        public FV getValue (String value) {
            for (FV fv : values)
                if (fv.label.equals(value))
                    return fv;
            return null;
        }

        public List<FV> getValues (Predicate<FV> pred) {
            return values.stream().filter(pred).collect(Collectors.toList());
        }

        public List<FV> getValuesWithPrefix (String prefix) {
            return getValues (fv -> fv.label.startsWith(prefix));
        }

        @JsonProperty(value="count")
        public int size () { return values.size(); }
        
        public void filter (String... labels) {
            Set<String> flabels = new HashSet<>();
            for (String l : labels)
                flabels.add(l);
            
            //Logger.debug("filter... "+flabels);
            Comparator<FV> fvcomp = (a,b) -> a.getPath().compareTo(b.getPath());
            Set<FV> nodes = new TreeSet<>(fvcomp);
            for (FV fv : values)
                Index.filter(nodes, fv, flabels);

            Set<FV> remove = new TreeSet<>(fvcomp);
            for (FV fv : values)
                Index.filter(fv, nodes, remove);
            /*
            Logger.debug("nodes... "+nodes);
            Logger.debug("remove... "+remove);
            Logger.debug("values... "+values);
            */
            // now remove all nodes not in nodes
            for (FV fv : remove) {
                if (fv.parent != null) {
                    boolean removed = fv.remove();
                    //Logger.debug("removing "+fv.getPath()+"..."+removed);
                }
                else {
                    boolean removed = values.remove(fv);
                    //Logger.debug("removing "+fv.getPath()+"..."+removed);
                }
            }
            //Logger.debug("final..."+values);
        }
        
        public void trim (int size) {
            int s = values.size();
            if (size > 0 && s > size) {
                List<FV> sub = new ArrayList<>(values.subList(0, size));
                values.clear();
                values.addAll(sub);
            }
        }

        public void trim (int thres, int size) {
            // this assumes sort() has already been done
            int s = values.size();
            if (s > size) {
                List<FV> sub = new ArrayList<>();
                for (FV fv : values) {
                    if (fv.count < thres || fv.specified) {
                        sub.add(fv);
                        if (sub.size() >= size)
                            break;
                    }
                }

                if (sub.isEmpty() || sub.size() < size) {
                    sub.clear();
                    sub.addAll(values.subList(0, size));
                }

                values.clear();
                values.addAll(sub);
            }
        }
        
        void sort () {
            Collections.sort(values, (fa, fb) -> {
                    int d = 0;
                    if (fa.specified == fb.specified)
                        d = fb.count - fa.count;
                    else if (fa.specified)
                        d = -1;
                    else if (fb.specified)
                        d = 1;
                    if (d == 0)
                        d = fa.label.compareTo(fb.label);
                    return d;
                });
            for (FV fv : values)
                Index.sort(fv);
        }
    }

    static void addDescendants (Set<FV> nodes, FV n) {
        nodes.add(n);
        for (FV child : n.children)
            addDescendants (nodes, child);
    }
    
    static void filter (Set<FV> nodes, FV n, Set<String> labels) {
        if (labels.contains(n.getPath())) {
            // add this node and all of its children and parents
            for (FV p = n; p != null; p = p.parent)
                nodes.add(p);
            n.specified = true;
            addDescendants (nodes, n);
        }
        else {
            for (FV child : n.children)
                filter (nodes, child, labels);
        }
    }

    static void filter (FV n, Set<FV> keep, Set<FV> remove) {
        if (!keep.contains(n)) {
            remove.add(n);
        }
        else {
            for (FV child : n.children)
                filter (child, keep, remove);
        }
    }
    
    /*
     * merge facets; facet names must be the same 
     */
    public static Facet merge (Facet... facets) {
        Facet merged = null;
        for (Facet f : facets) {
            if (merged == null) {
                merged = clone (f);
            }
            else if (merged.name.equals(f.name)) {
                for (FV fv : f.values)
                    merge (merged.values, fv);
            }
            else {
                Logger.warn("Facet \""+f.name+"\" not merged!");
            }
        }
        
        merged.sort();
        return merged;
    }

    public static Facet clone (Facet facet) {
        Facet clone = new Facet (facet.name);
        clone.display = facet.display;
        for (FV fv : facet.values) {
            clone.values.add(clone (fv));
        }
        return clone;
    }

    static FV clone (FV fv) {
        FV c = _clone (fv);
        clone (c, fv);
        return c;
    }
    
    static FV _clone (FV fv) {
        FV clone = new FV (fv.label, fv.count, fv.specified);
        clone.total = fv.total;
        clone.display = fv.display;
        return clone;
    }

    static void clone (FV clone, FV fv) {
        for (FV child : fv.children) {
            FV c = _clone (child);
            clone.add(c);
            clone (c, child);
        }
    }

    static void sort (FV fv) {
        Collections.sort(fv.children, (fa, fb) -> {
                int d = fb.count - fa.count;
                if (d == 0)
                    d = fa.label.compareTo(fb.label);
                return d;
            });
        for (FV child : fv.children)
            sort (child);
    }
    
    static void merge (List<FV> values, FV fval) {
        int pos = -1;
        for (int i = 0; i < values.size() && pos < 0; ++i) {
            FV fv = values.get(i);
            if (fv.label.equals(fval.label)) {
                fv.count += fval.count;
                if (fv.children.isEmpty()) {
                    for (FV fc : fval.children)
                        fv.add(clone (fc));
                }
                else {
                    for (FV fc : fval.children)
                        merge (fv.children, fc);
                }
                pos = i;
            }
        }
        
        if (pos < 0) {
            values.add(fval);
        }
    }

    public static class MatchedFragment {
        public final String fragment;
        public final String field;
        public final String text;

        public MatchedFragment (String text) {
            String f = null, t = null;
            // locate <b>...</b>
            int pos = text.indexOf("<b>");
            if (pos >= 0) {
                int bgn = pos;
                while (--bgn > 0 && text.charAt(bgn) != '>')
                    ;
                for (int i = bgn; --i >= 0; ) {
                    if (text.startsWith("fn=\"", i)) {
                        i += 4;
                        int j = i;
                        for (; j < bgn && text.charAt(j) != '"'; ++j)
                            ;
                        f = text.substring(i, j);
                        break;
                    }
                }
                
                pos = text.indexOf("</b>", pos);
                if (pos > 0) {
                    pos += 4;
                    bgn = Math.max(0, bgn);
                    for (int i = pos; i < text.length(); ++i) {
                        if (text.startsWith("</fld", i)) {
                            if (text.charAt(bgn) == '>')
                                ++bgn;
                            t = text.substring(bgn, i);
                            break;
                        }
                    }
                    
                    if (t == null) {
                        if (bgn < 0) bgn = 0;
                        if (text.charAt(bgn) == '>')
                            ++bgn;
                        t = text.substring(bgn);
                    }
                }
            }
            this.field = f;
            this.fragment = t;
            this.text = text;
        }

        public MatchedFragment (String field, String text) {
            this.fragment = text;
            this.text = text;
            this.field = field;
        }

        public String toString () {
            return "MatchedFragment{text="+text+",field="+field
                +",fragment="+fragment+"}";
        }
    }
    
    public static class ResultDoc {
        public final Document doc;
        public final Float score;
        public final int docId;
        public final IndexReader reader;
        public final FieldQuery fq;
        public final FastVectorHighlighter fvh;

        ResultDoc (Document doc, int docId, IndexReader reader,
                   Float score, FieldQuery fq, FastVectorHighlighter fvh) {
            this.doc = doc;
            this.score = score;
            this.docId = docId;
            this.reader = reader;
            this.fq = fq;
            this.fvh = fvh;
        }

        public String[] getFragments (String field, int length, int nfrags)
            throws IOException {
            return fvh.getBestFragments
                (fq, reader, docId, field, length, nfrags);
        }
    }

    static public class Concept implements Comparable<Concept> {
        public final String ui;
        public final String name;
        public final List<String> types = new ArrayList<>();
        public Number score;
        public String source;
        public Object context; // optional context for the concept

        public Concept (String ui, String name) {
            this (ui, name, null);
        }
        
        public Concept (String ui, String name, String type) {
            if (ui == null)
                throw new IllegalArgumentException
                    ("Concept can't have null UI!");
            this.ui = ui;
            this.name = name;
            if (type != null)
                types.add(type);
        }

        public boolean equals (Object obj) {
            if (obj instanceof Concept) {
                return ui.equals(((Concept)obj).ui);
            }
            return false;
        }

        public int compareTo (Concept c) {
            return ui.compareTo(c.ui);
        }

        public int hashCode () { return ui.hashCode(); }
    }

    public interface CacheableContent {
        String cacheKey ();
    }
    
    public interface SearchQuery extends CacheableContent {
        default String getField () {
            return null;
        }
        default Object getQuery () {
            return null;
        }
        default int skip () {
            return 0;
        }
        default int top () {
            return 10;
        }
        default int max () {
            return skip()+top();
        }
        default int fdim () {
            return 10;
        }
        default Map<String, Object> getFacets () {
            return Collections.emptyMap();
        }
        default List<Concept> getConcepts () {
            return Collections.emptyList();
        }
        // rewrite this SearchQuery into its native form
        default Query rewrite () {
            return new MatchNoDocsQuery ();
        }
    }

    public static class FacetQuery implements SearchQuery {
        public final Map<String, Object> facets = new TreeMap<>();
        public int fdim = 100; // facet dimension

        public FacetQuery () {
            this (100);
        }
        public FacetQuery (int fdim) {
            this.fdim = fdim;
        }
        public FacetQuery (String facet, Object value) {
            facets.put(facet, value);
        }
        public FacetQuery (Map<String, Object> facets) {
            this.facets.putAll(facets);
        }

        public String cacheKey () {
            List<String> values = new ArrayList<>();
            for (Map.Entry<String, Object> me : getFacets().entrySet()) {
                values.add(me.getKey());
                Object v = me.getValue();
                if (v instanceof String[]) {
                    String[] vals = (String[])v;
                    for (String s : vals)
                        values.add(s);
                }
                else if (v instanceof Object[]) {
                    Object[] vals = (Object[])v;
                    for (Object val : vals) {
                        if (val instanceof String[]) {
                            for (String s : (String[])val)
                                values.add(s);
                        }
                        else {
                            values.add((String)val);
                        }
                    }
                }
                else {
                    values.add((String)v);
                }
            }
            values.add(String.valueOf(skip()));
            values.add(String.valueOf(top()));
            values.add(String.valueOf(fdim));
            return FacetQuery.class.getName()
                +"/"+Util.sha1(values.toArray(new String[0]));
        }
        public Map<String, Object> getFacets () { return facets; }
        public int fdim () { return fdim; }
        public Query rewrite () {
            return new MatchAllDocsQuery ();
        }
    }

    public static class TextQuery extends FacetQuery {
        public final String field;
        public final String query;
        public final List<Concept> concepts = new ArrayList<>();
        public int slop = 1; // phrase slop (see QueryBuilder.createPhraseQuery)
        public int skip = 0;
        public int top = 10;

        public TextQuery () {
            this (null, null, null);
            top = 0;
        }
        public TextQuery (Map<String, Object> facets) {
            this (null, null, facets);
        }
        public TextQuery (String query) {
            this (null, query, null);
        }
        public TextQuery (String query, Map<String, Object> facets) {
            this (null, query, facets);
        }
        public TextQuery (String field, String query,
                          Map<String, Object> facets) {
            this.field = field;
            this.query = query;
            if (facets != null)
                this.facets.putAll(facets);
        }
        public TextQuery (TextQuery tq) {
            this (tq.field, tq.query, tq.facets);
            concepts.addAll(tq.concepts);
            slop = tq.slop;
            skip = tq.skip;
            top = tq.top;
        }

        public String getField () { return field; }
        public String getQuery () { return query; }
        public int skip () { return skip; }
        public int top () { return top; }
        //public int max () { return top + skip; }
        public List<Concept> getConcepts () { return concepts; }
        // subclass should override for specific implementation
        public Query rewrite () {
            if (query != null && field != null)
                return new TermQuery (new Term (field, query));
            else if (query != null) {
                try {
                    QueryParser qp = new QueryParser
                        (FIELD_TEXT, new StandardAnalyzer ());
                    return qp.parse(query);
                }
                catch (Exception ex) {
                    Logger.error("Can't parse query: "+query, ex);
                    return new MatchNoDocsQuery ();
                }
            }
            return new MatchAllDocsQuery ();
        }

        public String cacheKey () {
            List<String> values = new ArrayList<>();
            if (field != null) values.add(field);
            if (query != null) values.add(query.toLowerCase());
            values.add(super.cacheKey());
            values.add("slop="+slop);
            return TextQuery.class.getName()
                +"/"+Util.sha1(values.toArray(new String[0]));
        }
        
        public String toString () {
            return "TextQuery{key="+cacheKey()+",field="+field+",query="
                +query+",skip="+skip+",top="+top+",facets="+facets+"}";
        }
    }
    
    public static abstract class SearchResult {
        public final SearchQuery query;
        public int total; // total matches
        public final List<Facet> facets = new ArrayList<>();

        @JsonIgnore
        protected ScoreDoc lastDoc;
        
        protected SearchResult () {
            this ((SearchQuery)null);
        }
        
        protected SearchResult (SearchQuery query) {
            this.query = query;
        }

        protected SearchResult (SearchResult result) {
            this.query = result.query;
            this.total = result.total;
            this.facets.addAll(result.facets);
        }
        
        protected abstract boolean process
            (IndexSearcher searcher, ResultDoc doc);
        
        @JsonProperty(value="count")
        public int size () { return 0; }
        
        @JsonProperty(value="facets")
        public List<Facet> getFacets () { return facets; }

        public Facet getFacet (String name) {
            for (Facet f : facets)
                if (name.equalsIgnoreCase(f.name)
                    || name.equalsIgnoreCase(f.display))
                    return f;
            return null;
        }
        
        @JsonIgnore
        public boolean isEmpty () {
            return 0 == size () && facets.isEmpty();
        }

        public SearchResult clone () {
            return null; // can't clone an abstract class
        }

        // can be overriden by subclass to do post processing
        protected void postProcessing (IndexSearcher searcher)
            throws IOException {
        }
    }

    public static class TermVector
        implements org.apache.lucene.search.Collector {
        final String field;
        final Map<String, Integer> vector = new TreeMap<>();

        protected TermVector (String field) {
            if (field == null)
                throw new IllegalArgumentException
                    ("Term vector can't have null field!");
            this.field = field;
        }
        
        protected TermVector (IndexSearcher searcher,
                              String field, Term... terms) throws IOException {
            this.field = field;
            Query query;
            if (terms.length == 0) {
                query = new MatchAllDocsQuery ();
            }
            else {
                BooleanQuery.Builder builder = new BooleanQuery.Builder();
                for (Term t : terms) {
                    builder = builder.add(new TermQuery (t),
                                          BooleanClause.Occur.MUST);
                }
                query = builder.build();
            }
            //Logger.debug("#### TermVector: "+query);
            searcher.search(query, this);
        }

        protected TermVector (IndexSearcher searcher,
                              String field, Query query) throws IOException {
            this.field = field;
            if (query == null)
                query = new MatchAllDocsQuery ();
            searcher.search(query, this);
        }

        public void add (TermVector tv) {
            if (!field.equals(tv.field))
                throw new IllegalArgumentException
                    ("Operational not supported between incompatible "
                     +"term vectors!");
            
            for (Map.Entry<String, Integer> me : tv.vector.entrySet()) {
                Integer cnt = vector.get(me.getKey());
                if (cnt == null) cnt = 0;
                vector.put(me.getKey(), cnt + me.getValue());
            }
        }
        
        public String getField () { return field; }
        //@JsonAnyGetter
        public Map<String, Integer> getVector () { return vector; }

        @Override
        public boolean needsScores () { return false; }
        
        public LeafCollector getLeafCollector (final LeafReaderContext ctx)
            throws IOException {
            final int docBase = ctx.docBase;
            return new LeafCollector () {
                @Override
                public void collect (int doc) {
                    try {
                        Terms terms = ctx.reader().getTermVector(doc, field);
                        if (terms != null) {
                            TermsEnum en = terms.iterator();
                            for (BytesRef bref; (bref = en.next()) != null; ) {
                                String term = bref.utf8ToString();
                                Integer cnt = vector.get(term);
                                vector.put(term, cnt == null ? 1 : (cnt+1));
                            }
                        }
                    }
                    catch (IOException ex) {
                        Logger.error("can't get term vector for field "
                                     +field, ex);
                    }
                }
                
                public void setScorer (Scorer scorer) throws IOException {
                }
            };
        }
    }

    final protected FieldType tvFieldType;
    final protected File root;
    final protected Directory indexDir;
    final protected IndexWriter indexWriter;
    final protected Directory taxonDir;
    final protected DirectoryTaxonomyWriter taxonWriter;
    final protected FacetsConfig facetConfig;
    final protected SearcherManager searcherManager;
    
    protected Index (File dir) throws IOException {
        this.root = dir;
        
        File text = new File (dir, "text");
        text.mkdirs();
        indexDir = new NIOFSDirectory (text.toPath());
        indexWriter = new IndexWriter
            (indexDir, new IndexWriterConfig (new StandardAnalyzer ()));
        File taxon = new File (dir, "taxon");
        taxon.mkdirs();
        taxonDir = new NIOFSDirectory (taxon.toPath());
        taxonWriter = new DirectoryTaxonomyWriter (taxonDir);
        facetConfig = configFacets ();

        tvFieldType = new FieldType (TextField.TYPE_STORED);
        tvFieldType.setStoreTermVectors(true);
        tvFieldType.setStoreTermVectorPositions(true);
        tvFieldType.setStoreTermVectorPayloads(true);
        tvFieldType.setStoreTermVectorOffsets(true);
        tvFieldType.freeze();

        searcherManager = new SearcherManager
            (indexWriter, new SearcherFactory ());
    }

    /*
     * should be overriden by subclass!
     */
    protected FacetsConfig configFacets () {
        return new FacetsConfig ();
    }

    public File getDbFile () { return root; }
    public void close () throws Exception {
        Logger.debug("!! closing index "+getDbFile()+"..."+size ());
        searcherManager.close();
        IOUtils.close(indexWriter, indexDir, taxonWriter, taxonDir);
    }
    public int size () {
        return indexWriter.numDocs();
    }

    public String toString () {
        return "###"+getClass().getName()+": db="+root+" size="+size();
    }

    protected void add (Document doc) throws IOException {
        indexWriter.addDocument(facetConfig.build(taxonWriter, doc));
    }
    
    public Index addIndexes (File... dbs) throws IOException {
        DirectoryTaxonomyWriter.OrdinalMap map =
            new DirectoryTaxonomyWriter.MemoryOrdinalMap();
        for (File f : dbs) {
            if (!f.exists()) {
                Logger.error(f+": not exist!");
            }
            else {
                File text = new File (f, "text");
                if (text.exists()) {
                    Directory index = new NIOFSDirectory (text.toPath());
                    Directory taxon = new NIOFSDirectory
                        (new File (f, "taxon").toPath());
                    TaxonomyMergeUtils.merge
                        (index, taxon, map, indexWriter,
                         taxonWriter, facetConfig);
                    IOUtils.close(index, taxon);
                }
                else {
                    Logger.error(f+": not a valid pubmed index!");
                }
            }
        }
        return this;
    }

    void getFacetHierarchy (FV fv, Facets facets, String dim, int topN)
        throws IOException {
        FacetResult fr = facets.getTopChildren(topN, dim, fv.toPath());
        if (fr != null) {
            for (int i = 0; i < fr.labelValues.length; ++i) {
                LabelAndValue lv = fr.labelValues[i];
                FV node = new FV (lv.label, lv.value.intValue());
                fv.add(node);
                getFacetHierarchy (node, facets, dim, topN);
            }
        }
    }

    void getSelectedFacets (Map<String, Object> fmap,
                            List<Facet> lf, Facets facets) throws IOException {
        for (Map.Entry<String, Object> me : fmap.entrySet()) {
            FacetsConfig.DimConfig conf =
                facetConfig.getDimConfig(me.getKey());
            // for now just handle simple facets
            if (conf != null && !conf.hierarchical) {
                Object value = me.getValue();
                if (value instanceof String) {
                    String v = (String)value;
                    for (Facet f : lf) {
                        if (f.name.equals(me.getKey())) {
                            FV fv = f.getValue(v);
                            if (fv == null) {
                                // make sure selected facets show up
                                Number count = facets.getSpecificValue
                                    (me.getKey(), v);
                                Logger.debug("!!!! "+me.getKey()+"/"+v
                                             +" => "+count);
                                if (count != null && count.intValue() > 0)
                                    f.values.add
                                        (0, new FV (v, count.intValue(), true));
                            }
                            else {
                                fv.specified = true;
                            }
                        }
                    }
                }
            }
        }
    }
    
    protected List<Facet> toFacets (Facets facets, int topN)
        throws IOException {
        List<Facet> lf = new ArrayList<>();
        for (FacetResult fr : facets.getAllDims(2*topN)) {
            //Logger.debug("Facet: "+fr);
            if (fr != null) {
                FacetsConfig.DimConfig dimconf =
                    facetConfig.getDimConfig(fr.dim);
                Facet f = new Facet (fr.dim);
                if (dimconf.hierarchical) {
                    for (int i = 0; i < fr.labelValues.length; ++i) {
                        LabelAndValue lv = fr.labelValues[i];
                        FV root = new FV (lv.label, lv.value.intValue());
                        f.values.add(root);
                        getFacetHierarchy (root, facets, fr.dim, topN);
                    }
                }
                else {
                    for (int i = 0; i < fr.labelValues.length; ++i) {
                        LabelAndValue lv = fr.labelValues[i];
                        FV fv = new FV (lv.label, lv.value.intValue());
                        f.values.add(fv);
                    }
                }
                lf.add(f);
            }
        }
        return lf;
    }

    protected int search (SearchResult result, int maxHits) throws Exception {
        long start = System.currentTimeMillis();
        Map<String, Object> fmap = result.query.getFacets();
        Query query = result.query.rewrite();
        //Logger.debug("### Query: "+query+" MaxHits: "+maxHits+" Facets: "+fmap);
        
        try (IndexReader reader = DirectoryReader.open(indexWriter);
             TaxonomyReader taxonReader =
             new DirectoryTaxonomyReader (taxonWriter)) {
            
            FastVectorHighlighter fvh = new FastVectorHighlighter ();
            FieldQuery fq = fvh.getFieldQuery(query, reader);
            IndexSearcher searcher = new IndexSearcher (reader);

            int max = Math.max(1, Math.min(maxHits, reader.maxDoc()));
            Facets facets;
            TopDocs docs;
            if (fmap == null || fmap.isEmpty()) {
                FacetsCollector fc = new FacetsCollector ();
                if (result.lastDoc != null) {
                    Logger.debug("** searchAfter: "+result.lastDoc);
                    docs = FacetsCollector.searchAfter
                        (searcher, result.lastDoc, query, max, fc);
                }
                else {
                    docs = FacetsCollector.search(searcher, query, max, fc);
                }
                facets = new FastTaxonomyFacetCounts
                    (taxonReader, facetConfig, fc);
            }
            else {
                DrillDownQuery ddq = new DrillDownQuery (facetConfig, query);
                for (Map.Entry<String, Object> me : fmap.entrySet()) {
                    FacetsConfig.DimConfig conf =
                        facetConfig.getDimConfig(me.getKey());
                    if (conf != null) {
                        Object value = me.getValue();
                        if (value instanceof String[]) {
                            String[] values = (String[])value;
                            if (conf.hierarchical)
                                ddq.add(me.getKey(), values);
                            else {
                                for (String v : values)
                                    ddq.add(me.getKey(), v);
                            }
                        }
                        else if (value instanceof Object[]) {
                            Object[] values = (Object[])value;
                            for (Object v : values) {
                                if (v instanceof String[])
                                    ddq.add(me.getKey(), (String[])v);
                                else
                                    ddq.add(me.getKey(), (String)v);
                            }
                        }
                        else {
                            ddq.add(me.getKey(), (String) value);
                        }
                    }
                    else {
                        Logger.warn("Unknown facet: "+me.getKey());
                    }
                }
                
                DrillSideways sideway = new DrillSideways 
                    (searcher, facetConfig, taxonReader);
                DrillSideways.DrillSidewaysResult swresults =
                    result.lastDoc != null
                    ? sideway.search(result.lastDoc, ddq, max)
                    : sideway.search(ddq, max);

                facets = swresults.facets;
                docs = swresults.hits;
            }

            if (docs != null) {
                result.total = docs.totalHits;
                // don't fetch the doc if the caller doesn't want to
                if (maxHits > 0) {
                    int nd = 0;
                    for (; nd < docs.scoreDocs.length; ++nd) {
                        int docId = docs.scoreDocs[nd].doc;
                        ResultDoc rdoc = new ResultDoc
                            (searcher.doc(docId), docId, reader,
                             docs.scoreDocs[nd].score, fq, fvh);
                        if (!result.process(searcher, rdoc))
                            break;
                    }
                    
                    if (nd > 0)
                        result.lastDoc = docs.scoreDocs[nd-1];
                }
                
                result.facets.addAll(toFacets (facets, result.query.fdim()));
                // ensure selected facets show up in the results
                getSelectedFacets (fmap, result.facets, facets);
                result.postProcessing(searcher);
            }

            Logger.debug("### Query "+query+" executed in "
                         +String.format
                         ("%1$.3fs", (System.currentTimeMillis()-start)*1e-3)
                         +"..."+result.size()+" hit(s) (out of "
                         +result.total+" found)!");

            return result.size();
        }
    }

    void addFacetValues (Facet facet, Facets facets, String... values)
        throws IOException {
        List<String> path = new ArrayList<>();
        FV parent = null;
        for (String v : values) {
            path.add(v);
            Number count = facets.getSpecificValue
                (facet.name, path.toArray(new String[0]));
            if (count != null && count.intValue() > 0) {
                FV fv = new FV (v, count.intValue());
                if (parent != null)
                    parent.add(fv);
                else
                    facet.values.add(fv);
                parent = fv;
            }
        }
        if (parent != null)
            parent.specified = true;
    }

    protected void facets (SearchResult result) throws Exception {
        long start = System.currentTimeMillis();
        SearchQuery fq = result.query;
        Logger.debug("### Facet: fdim="+fq.fdim()+" "+fq.getFacets());
        try (IndexReader reader = DirectoryReader.open(indexWriter);
             TaxonomyReader taxon = new DirectoryTaxonomyReader (taxonWriter)) {
            IndexSearcher searcher = new IndexSearcher (reader);
            FacetsCollector fc = new FacetsCollector ();
            
            TopDocs docs = FacetsCollector.search
                (searcher, fq.rewrite(), 1, fc);
            Facets facets = new FastTaxonomyFacetCounts
                (taxon, facetConfig, fc);
            
            if (fq.getFacets().isEmpty())
                result.facets.addAll(toFacets (facets, fq.fdim()));
            else {
                for (Map.Entry<String, Object> me : fq.getFacets().entrySet()) {
                    FacetsConfig.DimConfig conf =
                        facetConfig.getDimConfig(me.getKey());
                    if (conf != null) {
                        Facet facet = new Facet (me.getKey());
                        Object value = me.getValue();
                        if (value instanceof String[]) {
                            String[] values = (String[])value;
                            if (conf.hierarchical) {
                                addFacetValues (facet, facets, values);
                            }
                            else {
                                for (String v : values)
                                    addFacetValues (facet, facets, v);
                            }
                        }
                        else if (value instanceof Object[]) {
                            Object[] values = (Object[])value;
                            for (Object v : values) {
                                if (v instanceof String[]) {
                                    addFacetValues (facet, facets, (String[])v);
                                }
                                else
                                    addFacetValues (facet, facets, (String)v);
                            }
                        }
                        else {
                            addFacetValues (facet, facets, (String) value);
                        }
                        result.facets.add(facet);
                    }
                    else {
                        Logger.warn("Unknown facet: "+me.getKey());
                    }
                }
            }
            result.postProcessing(searcher);
            
            Logger.debug("### Facets executed in "
                         +String.format
                         ("%1$.3fs", (System.currentTimeMillis()-start)*1e-3));
        }
    }

    protected TopSuggestDocs suggest (CompletionQuery query, int n)
        throws IOException {
        try (IndexReader reader = DirectoryReader.open(indexWriter, true)) {
            SuggestIndexSearcher searcher = new SuggestIndexSearcher (reader);
            TopSuggestDocs docs = searcher.suggest(query, n);
            return docs;
        }
    }

    public static TermVector createTermVector (String field) {
        return new TermVector (field);
    }
    
    protected TermVector termVector (String field, Term... terms)
        throws IOException {
        try (IndexReader reader = DirectoryReader.open(indexWriter)) {
            return new TermVector (new IndexSearcher (reader), field, terms);
        }
    }

    protected TermVector termVector (String field, Query query)
        throws IOException {
        try (IndexReader reader = DirectoryReader.open(indexWriter)) {
            return new TermVector (new IndexSearcher (reader), field, query);
        }
    }

    public void prefix (String field, String prefix, int n)
        throws IOException {
        PrefixCompletionQuery query = new PrefixCompletionQuery
            (indexWriter.getAnalyzer(), new Term (field, prefix));
        TopSuggestDocs.SuggestScoreDoc[] docs =
            suggest(query, n).scoreLookupDocs();
        Logger.debug("prefix: field="+field+" prefix="
                     +prefix+"..."+docs.length);
        for (TopSuggestDocs.SuggestScoreDoc d : docs) {
            Logger.debug("...key="+d.key+" context="+d.context);
        }
    }

    protected void addTextField (Document doc, String field,
                                 Object value) throws IOException {
        addTextField (doc, field, "", value);
    }
    
    protected void addTextField (Document doc, String field,
                                 String context, Object value)
        throws IOException {
        doc.add(new Field (FIELD_TEXT,
                           "<fld fn=\""+field+"\""+context
                           +">"+value+"</fld>", tvFieldType));
    }
    
    public static byte[] toCompressedBytes (JsonNode json) throws IOException {
        return toCompressedBytes (Json.mapper().writeValueAsBytes(json));
    }

    public static byte[] toCompressedBytes (byte[] data) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream (1000);
             GZIPOutputStream gzip = new GZIPOutputStream (bos);) {
            gzip.write(data, 0, data.length);
            gzip.close();
            return bos.toByteArray();
        }
    }

    public static byte[] getByteArray (Document doc, String field)
        throws IOException {
        BytesRef ref = doc.getBinaryValue(field);
        if (ref != null) {
            try (ByteArrayInputStream bis = new ByteArrayInputStream
                 (ref.bytes, ref.offset, ref.length);
                 ByteArrayOutputStream bos = new ByteArrayOutputStream (1024);
                 GZIPInputStream gzip = new GZIPInputStream (bis)) {
                byte[] buf = new byte[1024];
                for (int nb; (nb = gzip.read(buf, 0, buf.length)) != -1; ) {
                    bos.write(buf, 0, nb);
                }
                return bos.toByteArray();
            }
        }
        return null;
    }

    public static org.w3c.dom.Document getXmlDoc (Document doc, String field)
        throws IOException {
        byte[] xml = getByteArray (doc, field);
        if (xml != null) {
            try {
                org.w3c.dom.Document d =
                    DocumentBuilderFactory.newInstance().newDocumentBuilder()
                    .parse(new ByteArrayInputStream (xml));
                d.setXmlStandalone(true);
                return d;
            }
            catch (Exception ex) {
                Logger.error("Can't parse xml:\n"+new String (xml), ex);
            }
        }
        return null;
    }
    
    public static JsonNode[] getJson (Document doc, String field)
        throws IOException {
        BytesRef[] brefs = doc.getBinaryValues(field);
        List<JsonNode> json = new ArrayList<>();
        for (BytesRef ref : brefs) {
            try (ByteArrayInputStream bis = new ByteArrayInputStream
                 (ref.bytes, ref.offset, ref.length);
                 ByteArrayOutputStream bos = new ByteArrayOutputStream (1000);
                 GZIPInputStream gzip = new GZIPInputStream (bis)) {
                byte[] buf = new byte[1024];
                for (int nb; (nb = gzip.read(buf, 0, buf.length)) != -1; ) {
                    bos.write(buf, 0, nb);
                }
                JsonNode n = Json.mapper().readTree(bos.toByteArray());
                json.add(n);
            }
        }
        return json.toArray(new JsonNode[0]);
    }
    
    public static void main (String[] argv) throws Exception {
        if (argv.length < 2) {
            System.err.println("Index INDEXDB INDEXES...");
            System.exit(1);
        }
        try (Index index = new Index (new File (argv[0]))) {
            List<File> files = new ArrayList<>();
            for (int i = 1; i < argv.length; ++i)
                files.add(new File (argv[i]));
            index.addIndexes(files.toArray(new File[0]));
        }
    }
}
