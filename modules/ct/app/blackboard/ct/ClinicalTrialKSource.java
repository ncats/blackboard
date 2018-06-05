package blackboard.ct;

import java.io.*;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;
import java.util.regex.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.inject.Named;
import akka.stream.ActorMaterializer;
import akka.stream.ActorMaterializerSettings;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import play.Logger;
import play.libs.Json;
import play.libs.ws.*;
import play.inject.ApplicationLifecycle;
import play.libs.F;
import play.cache.*;
import akka.actor.ActorSystem;

import blackboard.*;
import play.mvc.BodyParser;

import static blackboard.KEntity.*;

@Singleton
public class ClinicalTrialKSource implements KSource {
    final WSClient wsclient;
    final KSourceProvider ksp;
    final CacheApi cache;
    final ClinicalTrialDb ctdb;
    
    @Inject
    public ClinicalTrialKSource (WSClient wsclient, CacheApi cache,
                                 @Named("ct") KSourceProvider ksp,
                                 ClinicalTrialFactory ctfac,
                                 ApplicationLifecycle lifecycle) {
        this.wsclient = wsclient;
        this.ksp = ksp;
        this.cache = cache;

        Map<String, String> props = ksp.getProperties();
        String param = props.get("db");
        if (param == null)
            throw new RuntimeException
                ("No db specified in ct configuration!");

        File db = new File (param);
        ctdb = ctfac.get(db);

        lifecycle.addStopHook(() -> {
                wsclient.close();
                return F.Promise.pure(null);
            });
        
        Logger.debug("$"+ksp.getId()+": "+ksp.getName()
                     +" initialized; provider is "+ksp.getImplClass());
    }

    public void execute (KGraph kgraph, KNode... nodes) {
        Logger.debug("$"+ksp.getId()
                     +": executing on KGraph "+kgraph.getId()
                     +" \""+kgraph.getName()+"\"");
    }

    public ClinicalTrialDb getClinicalTrialDb () { return ctdb; }
}
