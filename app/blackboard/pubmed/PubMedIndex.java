package blackboard.pubmed;

import play.Logger;
import blackboard.pubmed.*;
import blackboard.umls.MetaMap;
import blackboard.mesh.MeshDb;
import blackboard.mesh.Descriptor;

import java.io.*;
import java.util.List;
import java.util.ArrayList;
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

public class PubMedIndex implements AutoCloseable {
    public static final String FIELD_YEAR = "year";
    public static final String FIELD_CUI = "cui";
    public static final String FIELD_PMID = "pmid";
    public static final String FIELD_UI = "ui";
    public static final String FIELD_TEXT = "text";
    public static final String FIELD_TR = "tr"; // tree number";
    public static final String FIELD_MESH = "mesh";
    public static final String FIELD_CONCEPT = "concept";
    public static final String FIELD_SEMTYPE = "semtype";
    public static final String FIELD_SOURCE = "source";
    // MetaMap compressed json
    public static final String FIELD_MM_TITLE = "mm_title";
    public static final String FIELD_MM_ABSTRACT = "mm_abstract"; 
    
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
    
    public PubMedIndex (File dir) throws IOException {
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
        IndexSearcher searcher = null;
        try {
            searcher = new IndexSearcher
                (DirectoryReader.open(indexWriter, true));
            NumericRangeQuery<Long> query = NumericRangeQuery.newLongRange
                (FIELD_PMID, pmid, pmid, true, true);
            TopDocs hits = searcher.search(query, 1);
            return hits.totalHits > 0;
        }
        finally {
            if (searcher != null)
                IOUtils.close(searcher.getIndexReader());
        }
    }
    
    public boolean addIfAbsent (PubMedDoc d) throws IOException {
        if (!indexed (d.getPMID())) {
            add (d);
            return true;
        }
        return false;
    }

    public void add (PubMedDoc d) throws IOException {
        Logger.debug(d.getPMID()+": "+d.getTitle());
        Document doc = new Document ();
        doc.add(new LongField (FIELD_PMID, d.getPMID(), Field.Store.YES));
        doc.add(new Field (FIELD_TEXT, d.getTitle(), tvFieldType));
        JsonNode json = metamap (doc, d.getTitle());
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
                index.debug();
            }
        }
    }
}
