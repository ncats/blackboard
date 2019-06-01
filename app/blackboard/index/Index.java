package blackboard.index;

import play.Logger;
import play.libs.Json;

import javax.inject.Inject;
import java.io.*;
import java.util.*;
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

import javax.xml.parsers.DocumentBuilderFactory;
import com.google.inject.assistedinject.Assisted;

public class Index implements AutoCloseable, Fields {
    public static final int MAX_HITS = 1000;
    
    public static class FV {
        @JsonIgnore
        public FV parent;
        public Integer total;
        public String display;
        public final String label;
        public Integer count;
        public final List<FV> children = new ArrayList<>();

        protected FV (String label, Integer count) {
            this.label = label;
            this.count = count;
        }

        public FV add (FV node) {
            node.parent = this;
            children.add(node);
            return this;
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
        
        public String toString () {
            StringBuilder sb = new StringBuilder ();
            print (sb, this);
            return sb.toString();
        }
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

        @JsonProperty(value="count")
        public int size () { return values.size(); }

        void sort () {
            Collections.sort(values, (fa, fb) -> {
                    int d = fb.count - fa.count;
                    if (d == 0)
                        d = fa.label.compareTo(fb.label);
                    return d;
                });
            for (FV fv : values)
                Index.sort(fv);
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
        FV clone = new FV (fv.label, fv.count);
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

    protected static class ResultDoc {
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

    protected static abstract class SearchResult {
        @JsonIgnore
        public final int fdim;
        public int total; // total matches
        public final List<Facet> facets = new ArrayList<>();
        
        protected SearchResult () {
            this (10);
        }
        protected SearchResult (int fdim) {
            this.fdim = fdim;
        }
        
        protected abstract boolean process
            (IndexSearcher searcher, ResultDoc doc);
        
        @JsonProperty(value="count")
        public int size () { return 0; }
        
        @JsonProperty(value="facets")
        public List<Facet> getFacets () { return facets; }
        
        @JsonIgnore
        public boolean isEmpty () { return 0 == size (); }

        // can be overriden by subclass to do post processing
        protected void postProcessing (IndexSearcher searcher)
            throws IOException {
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

    protected List<Facet> toFacets (Facets facets, int topN)
        throws IOException {
        List<Facet> lf = new ArrayList<>();
        for (FacetResult fr : facets.getAllDims(topN)) {
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

    protected int search (Query query, SearchResult results) throws Exception {
        return search (query, null, results, MAX_HITS);
    }

    protected int search (Query query, SearchResult results, int maxHits)
        throws Exception {
        return search (query, null, results, maxHits);
    }

    protected int search (Query query, Map<String, Object> fmap,
                          SearchResult result) throws Exception {
        return search (query, fmap, result, MAX_HITS);
    }
    
    protected int search (Query query, Map<String, Object> fmap,
                          SearchResult result, int maxHits)
        throws Exception {
        long start = System.currentTimeMillis();
        Logger.debug("### Query: "+query+" MaxHits: "+maxHits+" Facets: "+fmap);
        
        try (IndexReader reader = DirectoryReader.open(indexWriter);
             TaxonomyReader taxonReader =
             new DirectoryTaxonomyReader (taxonWriter)) {
            
            FastVectorHighlighter fvh = new FastVectorHighlighter ();
            FieldQuery fq = fvh.getFieldQuery(query, reader);
            IndexSearcher searcher = new IndexSearcher (reader);
            FacetsCollector fc = new FacetsCollector ();

            int max = Math.max(1, Math.min(maxHits, reader.maxDoc()));
            Facets facets;
            TopDocs docs;
            if (fmap == null || fmap.isEmpty()) {
                docs = FacetsCollector.search(searcher, query, max, fc);
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
                    sideway.search(ddq, max);

                // collector
                FacetsCollector.search(searcher, ddq, max, fc);
                facets = swresults.facets;
                docs = swresults.hits;
            }

            if (docs != null) {
                result.total = docs.totalHits;
                for (int nd = 0; nd < docs.scoreDocs.length; ++nd) {
                    int docId = docs.scoreDocs[nd].doc;
                    ResultDoc rdoc = new ResultDoc
                        (searcher.doc(docId), docId, reader,
                         docs.scoreDocs[nd].score, fq, fvh);
                    if (!result.process(searcher, rdoc))
                        break;
                }
                result.facets.addAll(toFacets (facets, result.fdim));
                result.postProcessing(searcher);
            }

            Logger.debug("### Query executed in "
                         +String.format
                         ("%1$.3fs", (System.currentTimeMillis()-start)*1e-3)
                         +"..."+result.size()+" hit(s) (out of "
                         +result.total+" found)!");
            
            return result.size();
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
