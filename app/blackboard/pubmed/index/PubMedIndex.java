package blackboard.pubmed.index;

import play.Logger;
import play.libs.Json;
import blackboard.pubmed.*;
import blackboard.umls.MetaMap;
import blackboard.mesh.MeshDb;
import blackboard.mesh.Descriptor;

import javax.inject.Inject;
import java.io.*;
import java.util.*;
import java.util.zip.GZIPOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.concurrent.atomic.AtomicInteger;
import java.time.temporal.ChronoField;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

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

import gov.nih.nlm.nls.metamap.AcronymsAbbrevs;
import gov.nih.nlm.nls.metamap.ConceptPair;
import gov.nih.nlm.nls.metamap.Ev;
import gov.nih.nlm.nls.metamap.MatchMap;
import gov.nih.nlm.nls.metamap.Mapping;
import gov.nih.nlm.nls.metamap.MetaMapApi;
import gov.nih.nlm.nls.metamap.MetaMapApiImpl;
import gov.nih.nlm.nls.metamap.Negation;
import gov.nih.nlm.nls.metamap.PCM;
import gov.nih.nlm.nls.metamap.Phrase;
import gov.nih.nlm.nls.metamap.Position;
import gov.nih.nlm.nls.metamap.Result;
import gov.nih.nlm.nls.metamap.Utterance;

import com.google.inject.assistedinject.Assisted;

public class PubMedIndex implements AutoCloseable {
    public static final String FIELD_YEAR = "year";
    public static final String FIELD_CUI = "cui";
    public static final String FIELD_PMID = "pmid";
    public static final String FIELD_UI = "ui";
    public static final String FIELD_TEXT = "text";
    public static final String FIELD_TR = "tr"; // tree number";
    public static final String FIELD_MESH = "mesh";
    public static final String FIELD_TITLE = "title";
    public static final String FIELD_CONCEPT = "concept";
    public static final String FIELD_SEMTYPE = "semtype";
    public static final String FIELD_SOURCE = "source";
    // MetaMap compressed json
    public static final String FIELD_MM_TITLE = "mm_title";
    public static final String FIELD_MM_ABSTRACT = "mm_abstract"; 

    public static class MatchedDoc {
        public final Long pmid;
        public final String title;
        public final Integer year;
        public final List<String> fragments = new ArrayList<>();
        public final List<String> mesh = new ArrayList<>();
        public final Map<String, JsonNode> concepts = new TreeMap<>();

        protected MatchedDoc (Long pmid, String title, Integer year) {
            this.pmid = pmid;
            this.title = title;
            this.year = year;
        }
    }

    public static class FV {
        public FV parent;
        public Integer total;        
        public final String label;
        public final Integer count;
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
                sb.append(" ");
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
    }

    public static class SearchResult {
        public final List<Facet> facets = new ArrayList<>();
        public final List<MatchedDoc> docs = new ArrayList<>();

        protected SearchResult () {
        }
    }
    
    final FieldType tvFieldType;
    final File root;
    final Directory indexDir;
    final IndexWriter indexWriter;
    final Directory taxonDir;
    final DirectoryTaxonomyWriter taxonWriter;
    final FacetsConfig facetConfig;
    final SearcherManager searcherManager;
    final ObjectMapper mapper = new ObjectMapper ();
    MetaMap metamap = new MetaMap ();

    @Inject
    public PubMedIndex (@Assisted File dir) throws IOException {
        File text = new File (dir, "text");
        text.mkdirs();
        indexDir = new NIOFSDirectory (text.toPath());
        indexWriter = new IndexWriter
            (indexDir, new IndexWriterConfig (new StandardAnalyzer ()));
        File taxon = new File (dir, "taxon");
        taxon.mkdirs();
        taxonDir = new NIOFSDirectory (taxon.toPath());
        taxonWriter = new DirectoryTaxonomyWriter (taxonDir);
        facetConfig = new FacetsConfig ();
        facetConfig.setMultiValued(FIELD_TR, true);
        facetConfig.setHierarchical(FIELD_TR, true);
        facetConfig.setMultiValued(FIELD_MESH, true);
        facetConfig.setMultiValued(FIELD_SEMTYPE, true); // umls semantic types
        facetConfig.setMultiValued(FIELD_SOURCE, true); // umls sources
        facetConfig.setMultiValued(FIELD_CONCEPT, true);

        tvFieldType = new FieldType (TextField.TYPE_STORED);
        tvFieldType.setStoreTermVectors(true);
        tvFieldType.setStoreTermVectorPositions(true);
        tvFieldType.setStoreTermVectorPayloads(true);
        tvFieldType.setStoreTermVectorOffsets(true);
        tvFieldType.freeze();

        searcherManager = new SearcherManager
            (indexWriter, new SearcherFactory ());
        this.root = dir;
    }

    public File getDbFile () { return root; }
    public void close () throws Exception {
        searcherManager.close();
        IOUtils.close(indexWriter, indexDir, taxonWriter, taxonDir);
    }

    public void setMMPort (int port) {
        metamap = new MetaMap (port);
    }

    public String toString () {
        return "###PubMedIndex: db="+root+" size="+size();
    }

    protected JsonNode metamap (Document doc, String text) {
        JsonNode json = null;
        try {
            ArrayNode nodes = mapper.createArrayNode();
            for (Result r : metamap.annotate(text)) {
                for (AcronymsAbbrevs abrv : r.getAcronymsAbbrevsList()) {
                    for (String cui : abrv.getCUIList())
                        doc.add(new StringField
                                (FIELD_CUI, cui, Field.Store.NO));
                }
                
                for (Utterance utter : r.getUtteranceList()) {
                    for (PCM pcm : utter.getPCMList()) {
                        for (Mapping map : pcm.getMappingList())
                            for (Ev ev : map.getEvList()) {
                                doc.add(new StringField
                                        (FIELD_CUI, ev.getConceptId(),
                                         Field.Store.NO));
                                doc.add(new FacetField
                                        (FIELD_CONCEPT, ev.getConceptId()));
                                for (String t : ev.getSemanticTypes())
                                    doc.add(new FacetField (FIELD_SEMTYPE, t));
                                for (String s : ev.getSources())
                                    doc.add(new FacetField (FIELD_SOURCE, s));
                            }
                    }
                }
                json = MetaMap.toJson(r);
                //Logger.debug(">>> "+json);
                nodes.add(json);
            }
            
            json = nodes;
        }
        catch (Exception ex) {
            Logger.error("Can't annotate doc "
                         +doc.get(FIELD_PMID)+" with MetaMap", ex);
        }
        return json;
    }

    protected byte[] toCompressedBytes (JsonNode json) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream (1000);
             GZIPOutputStream gzip = new GZIPOutputStream (bos);) {
            byte[] data = mapper.writeValueAsBytes(json);
            gzip.write(data, 0, data.length);
            gzip.close();
            return bos.toByteArray();
        }
    }

    protected JsonNode[] toJson (Document doc, String field)
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
                JsonNode n = mapper.readTree(bos.toByteArray());
                json.add(n);
            }
        }
        return json.toArray(new JsonNode[0]);
    }

    public boolean indexed (Long pmid) throws IOException {
        try (IndexReader reader = DirectoryReader.open(indexWriter, true)) {
            IndexSearcher searcher = new IndexSearcher (reader);
            NumericRangeQuery<Long> query = NumericRangeQuery.newLongRange
                (FIELD_PMID, pmid, pmid, true, true);
            TopDocs hits = searcher.search(query, 1);
            return hits.totalHits > 0;
        }
    }
    
    public boolean addIfAbsent (PubMedDoc d) throws IOException {
        if (!indexed (d.getPMID())) {
            add (d);
            return true;
        }
        return false;
    }

    public PubMedIndex addIndexes (File... dbs) throws IOException {
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
    
    public PubMedIndex add (PubMedDoc d) throws IOException {
        Logger.debug(d.getPMID()+": "+d.getTitle());
        Document doc = new Document ();
        doc.add(new LongField (FIELD_PMID, d.getPMID(), Field.Store.YES));
        
        String title = d.getTitle();
        doc.add(new Field (FIELD_TEXT, title, tvFieldType));
        doc.add(new StoredField
                (FIELD_TITLE, new BytesRef (title.getBytes("utf8"))));
        JsonNode json = metamap (doc, title);
        if (json != null && json.size() > 0) {
            BytesRef ref = new BytesRef (toCompressedBytes (json));
            doc.add(new StoredField (FIELD_MM_TITLE, ref));
        }
        
        for (String abs : d.getAbstract()) {
            doc.add(new Field (FIELD_TEXT, abs, tvFieldType));
            json = metamap (doc, abs);
            if (json != null && json.size() > 0) {
                doc.add(new StoredField
                        (FIELD_MM_ABSTRACT,
                         new BytesRef (toCompressedBytes (json))));
            }
        }
        
        doc.add(new LongField
                (FIELD_YEAR, d.getDate().getYear(), Field.Store.YES));
        for (MeshHeading mh : d.getMeshHeadings()) {
            Descriptor desc = (Descriptor)mh.descriptor;
            doc.add(new StringField (FIELD_UI, desc.ui, Field.Store.YES));
            for (String tr : desc.treeNumbers) {
                Logger.debug("..."+tr);
                doc.add(new FacetField (FIELD_TR, tr.split("\\.")));
            }
            doc.add(new FacetField (FIELD_MESH, desc.name));
        }
        indexWriter.addDocument(facetConfig.build(taxonWriter, doc));
        
        return this;
    }

    public int size () {
        return indexWriter.numDocs();
    }

    protected void debug () throws IOException {
        IndexSearcher searcher = null;        
        try {
            searcher = new IndexSearcher
                (DirectoryReader.open(indexWriter, true));
            TaxonomyReader taxonReader =
                new DirectoryTaxonomyReader (taxonWriter);
            FacetsCollector fc = new FacetsCollector ();
            searcher.search(new MatchAllDocsQuery (), fc);
            Facets facets = new FastTaxonomyFacetCounts
                (taxonReader, facetConfig, fc);
            /*
            Logger.debug(facets.getTopChildren
                         (20, "tr", "D02.455.426.559.847.638".split("\\."))
                         .toString());
            */
            Logger.debug(facets.getTopChildren(20, FIELD_MESH).toString());
            Logger.debug(facets.getTopChildren(20, FIELD_CONCEPT).toString());
            Logger.debug(facets.getTopChildren(20, FIELD_SEMTYPE).toString());
            Logger.debug(facets.getTopChildren(20, FIELD_SOURCE).toString());

            /*
            IndexReader reader = searcher.getIndexReader();
            Document doc = reader.document(reader.maxDoc() - 1);
            JsonNode[] json = toJson (doc, FIELD_MM_TITLE);
            if (json.length > 0) {
                Logger.debug(">>> MetaMap Title Json:\n"+json[0]);
            }
            json = toJson (doc, FIELD_MM_ABSTRACT);
            for (JsonNode n : json) {
                Logger.debug(">>>> MetaMap Abstract Json:\n"+n);
            }
            */
            
            IOUtils.close(taxonReader);
        }
        finally {
            if (searcher != null)
                IOUtils.close(searcher.getIndexReader());
        }
    }

    MatchedDoc toDoc (Document doc) throws IOException {
        MatchedDoc md = null;
        IndexableField field = doc.getField(FIELD_PMID);
        if (field != null) {
            long id = field.numericValue().longValue();
            BytesRef ref = doc.getField(FIELD_TITLE).binaryValue();
            String title = new String (ref.bytes);
            int year = doc.getField(FIELD_YEAR).numericValue().intValue();
            md = new MatchedDoc (id, title, year);
            String[] mesh = doc.getValues(FIELD_UI);
            for (String ui : mesh)
                md.mesh.add(ui);
            /*
            JsonNode[] json = toJson (doc, FIELD_MM_TITLE);
            if (json != null && json.length > 0) {
                // there should only be one!
                md.concepts.put("title_mm", json[0]);
            }
            
            json = toJson (doc, FIELD_MM_ABSTRACT);
            if (json != null && json.length > 0) {
                md.concepts.put("abstract_mm", json.length == 1
                                ? json[0] : Json.toJson(json));
            }
            */
        }
        return md;
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

    List<Facet> toFacets (Facets facets, int topN) throws IOException {
        List<Facet> lf = new ArrayList<>();
        for (FacetResult fr : facets.getAllDims(topN)) {
            FacetsConfig.DimConfig dimconf = facetConfig.getDimConfig(fr.dim);
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
        return lf;
    }

    public SearchResult search (Query query) throws Exception {
        SearchResult result = null;
        
        try (IndexReader reader = DirectoryReader.open(indexWriter);
             TaxonomyReader taxonReader =
             new DirectoryTaxonomyReader (taxonWriter)) {
            result = new SearchResult ();
            
            FastVectorHighlighter fvh = new FastVectorHighlighter ();
            FieldQuery fq = fvh.getFieldQuery(query, reader);
            IndexSearcher searcher = new IndexSearcher (reader);
            FacetsCollector fc = new FacetsCollector ();
            TopDocs docs = FacetsCollector.search
                (searcher, query, reader.maxDoc(), fc);
            Facets facets = new FastTaxonomyFacetCounts
                (taxonReader, facetConfig, fc);
            for (int i = 0; i < docs.totalHits; ++i) {
                int docId = docs.scoreDocs[i].doc;
                MatchedDoc doc = toDoc (searcher.doc(docId));
                if (doc != null) {
                    String[] fragments = fvh.getBestFragments
                        (fq, reader, docId, FIELD_TEXT, 4000, 10);
                    if (fragments != null) {
                        Logger.debug("## found "+fragments.length
                                     +" fragment(s) in document "+doc.pmid+"!");
                        for (String f : fragments) {
                            Logger.debug(">>> "+f);
                            doc.fragments.add(f);
                        }
                    }
                    else {
                        Logger.error("Can't extract fragments from document "
                                     +doc.pmid+"!");
                    }
                    result.docs.add(doc);
                }
            }

            result.facets.addAll(toFacets (facets, 100));
            for (Facet f : result.facets)
                Logger.debug(f.toString());
            /*
            Logger.debug(facets.getTopChildren(20, FIELD_MESH).toString());
            Logger.debug(facets.getTopChildren
                         (20, "tr", "A11.251.860.180".split("\\."))
                         .toString());
            
            FacetResult fr = facets.getTopChildren(100, FIELD_MESH);
            for (int i = 0; i < fr.labelValues.length; ++i) {
                LabelAndValue lv = fr.labelValues[i];
                Logger.debug(i+": "+lv.label+" ["+lv.value+"]");
            }
            Logger.debug(facets.getTopChildren(20, FIELD_CONCEPT).toString());
            Logger.debug(facets.getTopChildren(20, FIELD_SEMTYPE).toString());
            Logger.debug(facets.getTopChildren(20, FIELD_SOURCE).toString());
            */
                
            return result;
        }
    }
    
    public SearchResult search (String text) throws Exception {
        QueryParser parser = new QueryParser
            (FIELD_TEXT, indexWriter.getAnalyzer());
        SearchResult result = search (parser.parse(text));
        Logger.debug("## searching for \""+text+"\"..."
                     +result.docs.size()+" hit(s)!");
        return result;
    }
    
    public static void main (String[] argv) throws Exception {
        if (argv.length < 3) {
            System.err.println("Usage: PubMedIndex INDEXDB MESHDB FILES...");
            System.exit(1);
        }

        Logger.debug("## IndexDb: "+argv[0]);
        Logger.debug("##  MeshDb: "+argv[1]);

        try (MeshDb mesh = new MeshDb (new File (argv[1]));
             PubMedIndex index = new PubMedIndex (new File (argv[0]))) {

            AtomicInteger count = new AtomicInteger ();
            PubMedSax pms = new PubMedSax (mesh, d -> {
                    try {
                        index.add(d);
                        Logger.debug(d.getPMID()+": "+d.getTitle());
                        if (count.incrementAndGet() > 100) {
                            throw new RuntimeException ("done!");
                        }
                    }
                    catch (IOException ex) {
                        Logger.error("Can't index document "+d.getPMID(), ex);
                    }
                });
            
            for (int i = 2; i < argv.length; ++i) {
                Logger.debug("indexing "+argv[i]+"...");
                try {
                    pms.parse(new java.util.zip.GZIPInputStream
                              (new FileInputStream (argv[i])));
                }
                catch (RuntimeException ex) {
                    
                }
                index.debug();
            }
            Logger.debug("## index "+argv[0]+": "+index.size()+" document(s)!");
        }
    }

    public static class Search {
        public static void main (String[] argv) throws Exception {
            if (argv.length < 2) {
                System.err.println("PubMedIndex$Search INDEXDB TERMS...");
                System.exit(1);
            }

            try (PubMedIndex index = new PubMedIndex (new File (argv[0]))) {
                for (int i = 1; i < argv.length; ++i) {
                    index.search(argv[i]);
                }
            }
        }
    }

    public static class Merge {
        public static void main (String[] argv) throws Exception {
            if (argv.length < 2) {
                System.err.println("PubMedIndex$Merge INDEXDB INDEXES...");
                System.exit(1);
            }
            try (PubMedIndex index = new PubMedIndex (new File (argv[0]))) {
                List<File> files = new ArrayList<>();
                for (int i = 1; i < argv.length; ++i)
                    files.add(new File (argv[i]));
                index.addIndexes(files.toArray(new File[0]));
            }
        }
    }
}
