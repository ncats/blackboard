package blackboard.idg.index;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
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
    
    protected Connection conn;
    protected ConcurrentMap<String, PreparedStatement> sqls =
        new ConcurrentHashMap<>();
    @Inject SyncCacheApi cache;
    
    @Inject
    public IDGIndex (@Assisted File dir) throws IOException {
        super (dir);
    }

    @Override
    protected FacetsConfig configFacets () {
        FacetsConfig fc = new FacetsConfig ();
        fc.setMultiValued(FACET_DSCAT, true);
        fc.setMultiValued(FACET_DSTYPE, true);
        fc.setMultiValued(FACET_DATASET, true);
        return fc;
    }

    public void setConnection (Connection conn) {
        if (conn == null)
            throw new IllegalArgumentException ("Database connection is null!");
        this.conn = conn;
    }
    public Connection getConnection () { return conn; }

    protected Document newDocument () {
        Document doc = new Document ();
        doc.add(new StringField (FIELD_INDEXER, VERSION, Field.Store.NO));
        return doc;
    }
    
    public IDGIndex addTarget (Target target) throws Exception {
        if (conn == null)
            throw new RuntimeException ("No database connection specified!");
        
        Long tcrdid = target.id;
        if (tcrdid == null)
            throw new IllegalArgumentException
                ("Input has no data for "+FIELD_TCRDID);
        
        Logger.debug("+++ adding target "+tcrdid+": "+ target.name);
        add (instrument (newDocument (), target));
        return this;
    }

    void addInfo (Document doc, Target target) throws IOException {
        if (target.name != null) {
            doc.add(new Field (FIELD_NAME, target.name, tvFieldType));
            addTextField (doc, FIELD_NAME, target.name);
        }
        
        doc.add(new FacetField (FACET_IDGTDL, target.idgtdl.toString()));
        if (target.gene != null) {
            doc.add(new StringField (FIELD_GENE, target.gene, Field.Store.YES));
            addTextField (doc, FIELD_GENE, target.gene);
        }
        
        doc.add(new StringField
                (FIELD_UNIPROT, target.uniprot, Field.Store.YES));
        addTextField (doc, FIELD_UNIPROT, target.uniprot);

        if (target.geneid != null)
            doc.add(new IntField (FIELD_GENEID,
                                  target.geneid, Field.Store.YES));
        if (target.chr != null) {
            addTextField (doc, FIELD_CHROMOSOME, target.chr);
            doc.add(new FacetField (FACET_CHROMOSOME, target.chr));
        }
        
        if (target.dtoid != null) {
            addTextField (doc, FIELD_DTOID, target.dtoid);
            doc.add(new StringField (FIELD_DTOID,
                                     target.dtoid, Field.Store.YES));
        }

        if (target.stringid != null) {
            addTextField (doc, FIELD_STRINGID, target.stringid);
            doc.add(new StringField (FIELD_STRINGID, target.stringid,
                                     Field.Store.YES));
        }
        
        if (target.novelty != null)
            doc.add(new DoubleField (FIELD_NOVELTY,
                                     target.novelty, Field.Store.YES));

        if (target.sequence != null)
            doc.add(new StoredField (FIELD_AASEQ, target.sequence));
    }

    void addTDL (Document doc, Target target) throws Exception {
        PreparedStatement pstm = sqls.computeIfAbsent
            ("tdl_info", k -> {
                try {
                    return conn.prepareStatement
                    ("select * from tdl_info where protein_id = ?");
                }
                catch (SQLException ex) {
                    ex.printStackTrace();
                }
                return null;
            });
        pstm.setLong(1, target.id);
        
        try (ResultSet rset = pstm.executeQuery()) {
            while (rset.next()) {
                String type = rset.getString("itype");
                String fname = type.replaceAll("\\s", "_");
                String sval = rset.getString("string_value");
                if (sval != null) {
                    switch (type) {
                    case "ChEMBL Selective Compound":
                        break;
                    case "TMHMM Prediction":
                        break;
                    case "IMPC Status":
                        if ("?".equals(sval))
                            sval = "Unknown";
                        doc.add(new FacetField ("@"+fname, sval));
                        break;
                    default:
                        addTextField (doc, fname, sval);
                        doc.add(new Field (fname, sval, tvFieldType));
                    }
                }
                else {
                    Double dval = rset.getDouble("number_value");
                    if (rset.wasNull()) {
                        Integer ival = rset.getInt("integer_value");
                        if (rset.wasNull()) {
                            Boolean bval = rset.getBoolean("boolean_value");
                            if (rset.wasNull()) {
                            }
                            else {
                                doc.add(new StringField
                                        (fname, bval.toString(),
                                         Field.Store.YES));
                            }
                        }
                        else { // int value
                            doc.add(new IntField
                                    (fname, ival, Field.Store.YES));
                        }
                    }
                    else { // double value
                        doc.add(new DoubleField (fname, dval, Field.Store.YES));
                    }
                }
            }
        }
    }

    void addDataset (Document doc, Target target) throws Exception {
        PreparedStatement pstm = sqls.computeIfAbsent
            ("dataset_info", k -> {
                try {
                    return conn.prepareStatement
                    ("select d.type,d.attr_count,d.attr_cdf,e.resource_group, "
                     +"e.attribute_type from target a, t2tc b, protein c, "
                     +"hgram_cdf d, gene_attribute_type e "
                     +"where a.id = b.target_id and b.protein_id = c.id "
                     +"and c.id = d.protein_id and e.name=d.type and c.id=?");
                }
                catch (SQLException ex) {
                    ex.printStackTrace();
                }
                return null;
            });
        pstm.setLong(1, target.id);

        try (ResultSet rset = pstm.executeQuery()) {
            while (rset.next()) {
                String ds = rset.getString("type");
                int count = rset.getInt("attr_count");
                double cdf = rset.getDouble("attr_cdf");
                String dscat = rset.getString("resource_group");
                String dstype = rset.getString("attribute_type");
                addTextField (doc, FIELD_DATASET, ds);
                doc.add(new FacetField (FACET_DATASET, ds));
                doc.add(new StoredField (FIELD_DATASET, ds));
                doc.add(new FacetField (FACET_DSCAT, dscat));
                doc.add(new StoredField (FIELD_DSCAT, dscat));
                doc.add(new FacetField (FACET_DSTYPE, dstype));
                doc.add(new StoredField (FIELD_DSTYPE, dstype));
            }
        }
    }

    void addExpression (Document doc, Target target) throws Exception {
    }
    
    protected Document instrument (Document doc, Target target)
        throws Exception {
        doc.add(new LongField (FIELD_TCRDID, target.id, Field.Store.YES));
        addInfo (doc, target);
        addTDL (doc, target);
        addDataset (doc, target);
        addExpression (doc, target);
        //Logger.debug(">>> "+doc);
        return doc;
    }
}
