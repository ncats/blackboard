package blackboard.pubmed.index;

import play.Logger;
import play.libs.Json;

import blackboard.pubmed.*;
import blackboard.mesh.MeshDb;
import blackboard.mesh.Descriptor;
import blackboard.index.MetaMapIndex;

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
import org.apache.lucene.index.*;
import org.apache.lucene.document.*;
import org.apache.lucene.facet.*;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.FastTaxonomyFacetCounts;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.search.*;
import org.apache.lucene.queryparser.classic.QueryParser;

import com.google.inject.assistedinject.Assisted;

public class PubMedIndex extends MetaMapIndex {
    // MetaMap compressed json
    public static final String FIELD_MM_TITLE = "mm_title";
    public static final String FIELD_MM_ABSTRACT = "mm_abstract"; 

    public static class MatchedDoc {
        public Long pmid;
        public String title;
        public Integer year;
        public List<String> fragments = new ArrayList<>();
        public List<String> mesh = new ArrayList<>();
        
        public JsonNode mm_title;
        public JsonNode mm_abstract;

        protected MatchedDoc () {
        }
        
        protected MatchedDoc (Long pmid, String title, Integer year) {
            this.pmid = pmid;
            this.title = title;
            this.year = year;
        }
    }

    public class SearchResult extends blackboard.index.Index.SearchResult {
        public final List<MatchedDoc> docs = new ArrayList<>();
        
        protected SearchResult () {
        }

        protected boolean process (blackboard.index.Index.ResultDoc rdoc) {
            try {
                MatchedDoc mdoc = toDoc (rdoc.doc);
                String[] frags = rdoc.getFragments(FIELD_TEXT, 500, 10);
                if (frags != null) {
                    for (String f : frags)
                        mdoc.fragments.add(f);
                }
                docs.add(mdoc);
            }
            catch (IOException ex) {
                Logger.error("Can't process doc "+rdoc.docId, ex);
            }
            return true;
        }
    }
    
    @Inject
    public PubMedIndex (@Assisted File dir) throws IOException {
        super (dir);
    }

    @Override
    protected FacetsConfig configFacets () {
        FacetsConfig fc = new FacetsConfig ();
        fc.setMultiValued(FIELD_TR, true);
        fc.setHierarchical(FIELD_TR, true);
        fc.setMultiValued(FIELD_MESH, true);
        fc.setMultiValued(FIELD_SEMTYPE, true); // umls semantic types
        fc.setMultiValued(FIELD_SOURCE, true); // umls sources
        fc.setMultiValued(FIELD_CONCEPT, true);
        fc.setMultiValued(FIELD_AUTHOR, true);
        fc.setMultiValued(FIELD_AFFILIATION, true);
        fc.setMultiValued(FIELD_PUBTYPE, true);
        fc.setMultiValued(FIELD_JOURNAL, true);
        fc.setMultiValued(FIELD_KEYWORD, true);
        fc.setMultiValued(FIELD_REFERENCE, true);
        return fc;
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

    protected Document instrument (Document doc, PubMedDoc d)
        throws IOException {
        doc.add(new LongField (FIELD_PMID, d.getPMID(), Field.Store.YES));
        addTextField (doc, FIELD_PMID, d.getPMID());
        
        String title = d.getTitle();
        if (title != null && !"".equals(title)) {
            addTextField (doc, FIELD_TITLE, title);
            doc.add(new Field (FIELD_TITLE, title, tvFieldType));
        }

        for (PubMedDoc.Author auth : d.authors) {
            if (auth.affiliations != null) {
                for (String affi : auth.affiliations)
                    doc.add(new FacetField (FIELD_AFFILIATION, affi));
            }
            doc.add(new FacetField (FIELD_AUTHOR, auth.getName()));
        }
        doc.add(new FacetField (FIELD_JOURNAL, d.journal));
        
        for (blackboard.mesh.Entry e : d.pubtypes)
            doc.add(new FacetField (FIELD_PUBTYPE, e.ui));
        
        for (String k : d.keywords)
            doc.add(new FacetField (FIELD_KEYWORD, k));
        
        for (String abs : d.getAbstract())
            addTextField (doc, "abstract", abs);

        doc.add(new LongField
                (FIELD_YEAR, d.getDate().getYear(), Field.Store.YES));
        for (MeshHeading mh : d.getMeshHeadings()) {
            Descriptor desc = (Descriptor)mh.descriptor;
            doc.add(new StringField (FIELD_UI, desc.ui, Field.Store.YES));
            addTextField (doc, FIELD_UI, desc.ui);
            for (String tr : desc.treeNumbers) {
                Logger.debug("..."+tr);
                doc.add(new FacetField (FIELD_TR, tr.split("\\.")));
            }
            doc.add(new FacetField (FIELD_MESH, desc.ui));
            addTextField (doc, FIELD_MESH, desc.name);
        }

        if (d.pmc != null) {
            doc.add(new StringField (FIELD_PMC, d.pmc, Field.Store.YES));
            addTextField (doc, FIELD_PMC, d.pmc);
        }
        if (d.doi != null) {
            doc.add(new StringField (FIELD_DOI, d.doi, Field.Store.YES));
        }

        for (PubMedDoc.Reference ref : d.references) {
            if (ref.pmids != null)
                for (Long id : ref.pmids)
                    doc.add(new FacetField
                            (FIELD_REFERENCE, String.valueOf(id)));
        }
        
        /*
         * now do metamap
         */
        JsonNode json = metamap (doc, title);
        if (json != null && json.size() > 0) {
            BytesRef ref = new BytesRef (toCompressedBytes (json));
            doc.add(new StoredField (FIELD_MM_TITLE, ref));
        }
        
        for (String abs : d.getAbstract()) {
            json = metamap (doc, abs);
            if (json != null && json.size() > 0) {
                doc.add(new StoredField
                        (FIELD_MM_ABSTRACT,
                         new BytesRef (toCompressedBytes (json))));
            }
        }
        
        return doc;
    }

    protected void add (Document doc) throws IOException {
        indexWriter.addDocument(facetConfig.build(taxonWriter, doc));
    }

    protected Document newDocument () {
        Document doc = new Document ();
        doc.add(new StringField
                (FIELD_INDEXER, getClass().getName(), Field.Store.YES));
        return doc;
    }
    
    public PubMedIndex add (PubMedDoc d) throws IOException {
        Logger.debug(d.getPMID()+": "+d.getTitle());
        add (instrument (newDocument (), d));
        return this;
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
            FacetResult fr = facets.getTopChildren(20, FIELD_MESH);
            if (fr != null)
                Logger.debug(fr.toString());
            fr = facets.getTopChildren(20, FIELD_CONCEPT);
            if (fr != null)
                Logger.debug(fr.toString());
            fr = facets.getTopChildren(20, FIELD_SEMTYPE);
            if (fr != null)
                Logger.debug(fr.toString());
            fr = facets.getTopChildren(20, FIELD_SOURCE);
            if (fr != null)
                Logger.debug(fr.toString());
            fr = facets.getTopChildren(20, FIELD_AUTHOR);
            if (fr != null)
                Logger.debug(fr.toString());
            fr = facets.getTopChildren(20, FIELD_AFFILIATION);
            if (fr != null)
                Logger.debug(fr.toString());
            fr = facets.getTopChildren(20, FIELD_REFERENCE);
            if (fr != null)
                Logger.debug(fr.toString());
            fr = facets.getTopChildren(20, FIELD_KEYWORD);
            if (fr != null)
                Logger.debug(fr.toString());

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
        return toDoc (new MatchedDoc (), doc);
    }
    
    MatchedDoc toDoc (MatchedDoc md, Document doc) throws IOException {
        IndexableField field = doc.getField(FIELD_PMID);
        if (field != null) {
            md.pmid = field.numericValue().longValue();
            md.title = doc.get(FIELD_TITLE);
            md.year = doc.getField(FIELD_YEAR).numericValue().intValue();
            String[] mesh = doc.getValues(FIELD_UI);
            for (String ui : mesh)
                md.mesh.add(ui);
            JsonNode[] json = toJson (doc, FIELD_MM_TITLE);
            if (json != null && json.length > 0) {
                md.mm_title = json[0]; // there should only be one!
            }
            
            json = toJson (doc, FIELD_MM_ABSTRACT);
            if (json != null && json.length > 0) {
                md.mm_abstract = json.length == 1
                    ? json[0] : Json.toJson(json);
            }
        }
        else {
            md = null;
        }
        return md;
    }

    public SearchResult search (Query query) throws Exception {
        SearchResult results = new SearchResult ();
        search (query, results);
        /*
        for (Facet f : results.facets)
            Logger.debug(f.toString());

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
        return results;
    }
    
    public SearchResult search (String text) throws Exception {
        QueryParser parser = new QueryParser
            (FIELD_TEXT, indexWriter.getAnalyzer());
        SearchResult result = search (parser.parse(text));
        Logger.debug("## searching for \""+text+"\"..."
                     +result.docs.size()+" hit(s)!");
        return result;
    }

    public SearchResult search (String field, String term) throws Exception {
        SearchResult result = search (new TermQuery (new Term (field, term)));
        Logger.debug("## searching for "+field+":"+term+"..."
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
                    SearchResult result = index.search(argv[i]);
                    for (MatchedDoc d : result.docs) {
                        System.out.println(d.pmid+": "+d.title);
                        for (String f : d.fragments)
                            System.out.println("..."+f);
                        //System.out.println("title_mm: "+d.mm_title);
                        System.out.println();
                    }

                    for (Facet f : result.facets)
                        Logger.debug(f.toString());
                }
            }
        }
    }
}
