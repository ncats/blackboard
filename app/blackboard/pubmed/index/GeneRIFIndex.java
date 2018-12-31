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
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.search.*;
import org.apache.lucene.queryparser.classic.QueryParser;

import com.google.inject.assistedinject.Assisted;

public class GeneRIFIndex extends PubMedIndex {
    public static final String FIELD_GENERIF = "generif";
    public static final String FIELD_MM_GENERIF = "mm_generif";
    
    public static class GeneRIFDoc extends MatchedDoc {
        public String gene;
        public String text;
        public JsonNode mm_text;

        protected GeneRIFDoc () {
        }
        protected GeneRIFDoc (String gene, String text) {
            this.gene = gene;
            this.text = text;
        }
    }
    
    public class SearchResult extends PubMedIndex.SearchResult {
        protected SearchResult () {
        }

        @Override
        protected boolean process (blackboard.index.Index.ResultDoc rdoc) {
            try {
                GeneRIFDoc doc = new GeneRIFDoc
                    (rdoc.doc.get(FIELD_GENE), rdoc.doc.get(FIELD_GENERIF));
                toDoc (doc, rdoc.doc);
                
                String[] frags = rdoc.getFragments(FIELD_TEXT, 500, 10);
                if (frags != null) {
                    for (String f : frags)
                        doc.fragments.add(f);
                }
                
                JsonNode[] json = toJson (rdoc.doc, FIELD_MM_GENERIF);
                if (json != null && json.length > 0)
                    doc.mm_text = json[0];
                docs.add(doc);
            }
            catch (IOException ex) {
                Logger.error("Can't process doc "+rdoc.docId, ex);
            }
            return true;
        }
    }

    @Inject
    public GeneRIFIndex (@Assisted File dir) throws IOException {
        super (dir);
    }

    protected Document instrument (Document doc, PubMedDoc d, String gene,
                                   String text) throws IOException {
        doc.add(new StringField (FIELD_GENE, gene, Field.Store.YES));
        addTextField (doc, FIELD_GENE, gene);
        
        doc.add(new Field (FIELD_GENERIF, text, tvFieldType));
        addTextField (doc, FIELD_GENERIF, text);
        
        instrument (doc, d);
        
        JsonNode json = metamap (doc, text);
        if (json != null && json.size() > 0) {
            BytesRef ref = new BytesRef (toCompressedBytes (json));
            doc.add(new StoredField (FIELD_MM_GENERIF, ref));
        }
        return doc;
    }

    public GeneRIFIndex add (PubMedDoc d, String gene, String text)
        throws IOException {
        Logger.debug(d.getPMID()+" "+gene+": "+text);
        add (instrument (newDocument (), d, gene, text));
        return this;
    }
    
    public boolean addIfAbsent (PubMedDoc doc, String gene, String text)
        throws IOException {
        boolean added = false;
        try (IndexReader reader = DirectoryReader.open(indexWriter, true)) {
            IndexSearcher searcher = new IndexSearcher (reader);
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            builder.add(new TermQuery (new Term (FIELD_GENE, gene)),
                        BooleanClause.Occur.MUST)
                .add(NumericRangeQuery.newLongRange
                     (FIELD_PMID, doc.getPMID(), doc.getPMID(), true, true),
                     BooleanClause.Occur.FILTER);
            
            TopDocs hits = searcher.search(builder.build(), 1);
            if (hits.totalHits == 0) {
                add (doc, gene, text);
                added = true;
            }
        }
        return added;
    }

    @Override
    public SearchResult search (Query query) throws Exception {
        SearchResult results = new SearchResult ();
        search (query, results);
        return results;
    }

    @Override
    public SearchResult search (String text) throws Exception {
        QueryParser parser = new QueryParser
            (FIELD_TEXT, indexWriter.getAnalyzer());
        SearchResult result = search (parser.parse(text));
        Logger.debug("## searching for \""+text+"\"..."
                     +result.docs.size()+" hit(s)!");
        return result;
    }

    @Override
    public SearchResult search (String field, String term) throws Exception {
        SearchResult result = search (new TermQuery (new Term (field, term)));
        Logger.debug("## searching for "+field+":"+term+"..."
                     +result.docs.size()+" hit(s)!");
        return result;
    }
    

    public static void main (String[] argv) throws Exception {
        if (argv.length < 2) {
                System.err.println("GeneRIFIndex INDEXDB TERMS...");
                System.exit(1);
        }
        
        try (GeneRIFIndex index = new GeneRIFIndex (new File (argv[0]))) {
            for (int i = 1; i < argv.length; ++i) {
                SearchResult result = index.search(argv[i]);
                for (Facet f : result.facets)
                    Logger.debug(f.toString());
                
                for (MatchedDoc md : result.docs) {
                    GeneRIFDoc d = (GeneRIFDoc)md;
                    System.out.println(d.gene+" "+d.pmid+": "
                                       +d.title+"\n"+d.text);
                    for (String f : d.fragments)
                        System.out.println("..."+f);
                    //System.out.println("title_mm: "+d.mm_title);
                    System.out.println();
                }
            }
        }
    }
}
