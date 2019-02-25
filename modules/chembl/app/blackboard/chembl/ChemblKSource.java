
package blackboard.chembl;

import blackboard.KGraph;
import blackboard.KNode;
import blackboard.KSource;
import blackboard.KSourceProvider;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import akka.actor.ActorSystem;
import play.Logger;
import play.libs.Json;
import play.libs.ws.*;
import play.inject.ApplicationLifecycle;
import play.libs.F;
import play.cache.*;
import play.db.NamedDatabase;
import play.db.Database;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.lang.reflect.Array;
import java.util.*;
import java.sql.*;
import java.io.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ConcurrentHashMap;

import blackboard.pubmed.PubMedKSource;
import blackboard.pubmed.PubMedDoc;
import blackboard.pubmed.MeshHeading;
import blackboard.mesh.Descriptor;
import static blackboard.KEntity.*;

@Singleton
public class ChemblKSource implements KSource {
    public final KSourceProvider ksp;
    public final Database db;
    public final String dbver;
    public final PubMedKSource pubmed;
    
    private final SyncCacheApi cache;
    
    @Inject
    public ChemblKSource (SyncCacheApi cache,
                          @Named("chembl") KSourceProvider ksp,
                          @NamedDatabase("chembl") Database db,
                          PubMedKSource pubmed,
                          ApplicationLifecycle lifecycle) {
        this.ksp = ksp;
        this.db = db;
        this.pubmed = pubmed;
        this.cache = cache;
        
        try (Connection con = db.getConnection()) {
            Statement stm = con.createStatement();
            ResultSet rset = stm.executeQuery("select * from version");
            if (rset.next()) {
                dbver = rset.getString("name");
            }
            else {
                dbver = "Unknown version";
            }
            rset.close();
            stm.close();
        }
        catch (SQLException ex) {
            Logger.error("Can't initialize "+ksp.getId()+"!", ex);
            throw new RuntimeException
                ("ChEMBL knowledge source is unusable without "
                 +"a valid database connection!");
        }
        
        lifecycle.addStopHook(() -> {
            db.shutdown();
            return CompletableFuture.completedFuture(null);
        });
        
        Logger.debug("$"+ksp.getId()+": "+ksp.getName()
                     +" initialized; provider is "+ksp.getImplClass()
                     +"; dbver = "+dbver);
    }

    public void fetchDocs () throws Exception {
        File file = new File ("chembl_mesh.txt");
        if (!file.exists()) {
            PrintStream ps = new PrintStream (new FileOutputStream (file));
            try (Connection con = db.getConnection()) {
                Statement stm = con.createStatement();
                ResultSet rset = stm.executeQuery
                    ("select pubmed_id from docs where pubmed_id is not null");
                while (rset.next()) {
                    long pmid = rset.getLong(1);
                    PubMedDoc doc = pubmed.getPubMedDoc(pmid);
                    if (doc != null) {
                        for (MeshHeading mh : doc.getMeshHeadings()) {
                            Descriptor desc = (Descriptor)mh.descriptor;
                            for (String tr : desc.treeNumbers) {
                                ps.println(pmid+"\t"+desc.ui+"\t"+tr);
                            }
                        }
                    }
                    else {
                        Logger.warn(pmid+": can't retrieve pubmed doc!");
                    }
                }
                rset.close();
            }
            ps.close();
        }
    }

    @Override
    public void execute(KGraph kgraph, KNode... nodes) {
        Logger.debug("$"+ksp.getId()
                +": executing on KGraph "+kgraph.getId()
                +" \""+kgraph.getName()+"\"");
    }
}

