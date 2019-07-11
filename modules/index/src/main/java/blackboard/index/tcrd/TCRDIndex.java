package blackboard.index.tcrd;

import java.io.*;
import java.util.*;
import java.sql.*;

import play.Logger;
import play.libs.Json;
import play.cache.SyncCacheApi;
import play.db.NamedDatabase;
import play.db.Database;

//import blackboard.tcrd.*;
import blackboard.index.Index;

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
import blackboard.utils.Util;

public class TCRDIndex extends Index implements TCRDFields {
    public static final String VERSION = "TCRDIndex-v1";

    @Inject
    protected @NamedDatabase("tcrd") Database tcrd;
    
    @Inject
    public TCRDIndex (@Assisted File dir) throws IOException {
        super (dir);
    }

    @Override
    protected FacetsConfig configFacets () {
        FacetsConfig fc = new FacetsConfig ();
        return fc;
    }

    public void setDatabase (Database db) {
        this.tcrd = db;
    }
    public Database getDatabase () { return tcrd; }

    public void build () throws Exception {
        build (0);
    }
    
    public void build (int limits) throws Exception {
        if (tcrd == null)
            throw new RuntimeException ("No TCRD Database avaiable!");
        try (Connection con = tcrd.getConnection();
             Statement stm = con.createStatement()) {
            ResultSet rset = stm.executeQuery
                ("select a.target_id,a.protein_id,b.name\n"
                 +"from t2tc a "
                 +"     join (target b, protein c)\n"
                 +"on (a.target_id = b.id and a.protein_id = c.id)\n"
                 +"left join tinx_novelty d\n"
                 +"    on d.protein_id = a.protein_id \n"
                 //+"where c.id in (18204,862,74,6571)\n"
                 //+"where a.target_id in (875)\n"
                 //+"where c.uniprot = 'P01375'\n"
                 //+"where b.tdl in ('Tclin','Tchem')\n"
                 //+"where b.idgfam = 'kinase'\n"
                 //+" where c.uniprot in ('Q96K76','Q6PEY2')\n"
                 //+"where b.idg2=1\n"
                 +"order by d.score desc, c.id\n"
                 +(limits > 0 ? ("limit "+limits) : ""));
            while (rset.next()) {
                long protid = rset.getLong("protein_id");
                long targetid = rset.getLong("target_id");
                if (rset.wasNull()) {
                    Logger.warn("Not a protein target: "+targetid);
                    continue;
                }
                Map<String, Object> data = new LinkedHashMap<>();
                data.put(FIELD_TCRDID, targetid);
                data.put(FIELD_NAME, rset.getString("name"));
                addTarget (data);
            }
            rset.close();
        }
    }

    protected void addTarget (Map<String, Object> data) throws Exception {
        Logger.debug("+++ adding target "+data.get(FIELD_TCRDID)+": "+
                     data.get(FIELD_NAME));
    }
}
