package blackboard.idg.index;

import java.io.*;
import java.util.*;
import java.sql.*;

import play.Logger;
import play.libs.Json;
import play.cache.SyncCacheApi;
import play.db.NamedDatabase;
import play.db.Database;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.lucene.store.*;
import org.apache.lucene.index.*;
import org.apache.lucene.document.*;
import org.apache.lucene.facet.*;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.FastTaxonomyFacetCounts;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.QueryBuilder;
import org.apache.lucene.search.*;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.suggest.document.*;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.Analyzer;

import com.google.inject.assistedinject.Assisted;
import blackboard.index.Index;
import blackboard.utils.Util;
import blackboard.idg.*;

public class IDGIndex extends Index implements IDGFields {
    public static final String VERSION = "IDGIndex-v1";
    
    public static class SearchResult
        extends blackboard.index.Index.SearchResult {
        
        protected SearchResult (SearchQuery query) {
            super (query);
        }
        protected SearchResult (SearchResult result) {
            super (result);
        }

        protected boolean process (IndexSearcher searcher,
                                   blackboard.index.Index.ResultDoc rdoc) {
            return true;
        }        
    }
    
    protected Database db;
    @Inject SyncCacheApi cache;
    
    @Inject
    public IDGIndex (@Assisted File dir) throws IOException {
        super (dir);
    }

    @Override
    protected FacetsConfig configFacets () {
        FacetsConfig fc = new FacetsConfig ();
        return fc;
    }

    public void setDatabase (Database db) {
        this.db = db;
    }
    public Database getDatabase () { return db; }

    protected Document newDocument () {
        Document doc = new Document ();
        doc.add(new StringField (FIELD_INDEXER, VERSION, Field.Store.NO));
        return doc;
    }
    
    public IDGIndex addTarget (Map<String, Object> data) throws Exception {
        if (db == null)
            throw new RuntimeException ("No database specified!");
        
        Long tcrdid = (Long) data.get(FIELD_TCRDID);
        if (tcrdid == null)
            throw new IllegalArgumentException
                ("Input has no data for "+FIELD_TCRDID);
        
        Logger.debug("+++ adding target "+tcrdid+": "+ data.get(FIELD_NAME));
        add (instrument (newDocument (), tcrdid, data));
        return this;
    }

    void addCore (Document doc, Map<String, Object> data) throws IOException {
        doc.add(new Field (FIELD_NAME,
                           (String)data.get(FIELD_NAME), tvFieldType));
        addTextField (doc, FIELD_NAME, data.get(FIELD_NAME));
        doc.add(new FacetField (FACET_IDGTDL, (String)data.get(FIELD_IDGTDL)));
        String gene = (String)data.get(FIELD_GENE);
        if (gene != null) {
            doc.add(new StringField (FIELD_GENE, gene, Field.Store.YES));
            addTextField (doc, FIELD_GENE, gene);
        }
        
        doc.add(new StringField
                (FIELD_UNIPROT, (String)data.get(FIELD_UNIPROT),
                 Field.Store.YES));
        addTextField (doc, FIELD_UNIPROT, data.get(FIELD_UNIPROT));
        doc.add(new IntField (FIELD_GENEID, (Integer)data.get(FIELD_GENEID),
                              Field.Store.YES));
        String chr = (String)data.get(FIELD_CHROMOSOME);
        if (chr != null) {
            addTextField (doc, FIELD_CHROMOSOME, chr);
            doc.add(new FacetField (FACET_CHROMOSOME, chr));
        }
        
        String dtoid = (String)data.get(FIELD_DTOID);
        if (dtoid != null) {
            addTextField (doc, FIELD_DTOID, dtoid);
            doc.add(new StringField (FIELD_DTOID, dtoid, Field.Store.YES));
        }

        String stringid = (String)data.get(FIELD_STRINGID);
        if (stringid != null) {
            addTextField (doc, FIELD_STRINGID, stringid);
            doc.add(new StringField (FIELD_STRINGID, stringid,
                                     Field.Store.YES));
        }
        
        if (data.containsKey(FIELD_NOVELTY)) {
            Double novelty = (Double)data.get(FIELD_NOVELTY);
            doc.add(new DoubleField (FIELD_NOVELTY, novelty, Field.Store.YES));
        }
        doc.add(new StoredField (FIELD_AASEQ, (String)data.get(FIELD_AASEQ)));
    }
    
    protected Document instrument (Document doc, Long tcrdid,
                                   Map<String, Object> data) throws Exception {
        doc.add(new LongField (FIELD_TCRDID, tcrdid, Field.Store.YES));
        addCore (doc, data);
        
        return doc;
    }
}
