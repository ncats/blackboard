package blackboard.mesh;


import java.io.*;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Named;
import akka.stream.ActorMaterializer;
import akka.stream.ActorMaterializerSettings;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeCreator;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import play.Logger;
import play.libs.Json;
import play.libs.ws.*;
import play.inject.ApplicationLifecycle;
import play.libs.F;
import play.cache.*;
import akka.actor.ActorSystem;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;

import blackboard.*;
import play.mvc.BodyParser;

import static blackboard.KEntity.*;

public class MeshKSource implements KSource {
    final WSClient wsclient;
    final KSourceProvider ksp;
    final CacheApi cache;
    final MeshDb mesh;
    
    @Inject
    public MeshKSource (WSClient wsclient, CacheApi cache,
                        @Named("mesh") KSourceProvider ksp,
                        ApplicationLifecycle lifecycle) {
        this.wsclient = wsclient;
        this.ksp = ksp;
        this.cache = cache;

        Map<String, String> props = ksp.getProperties();
        String param = props.get("db");
        if (param == null)
            throw new RuntimeException
                ("No db specified in mesh configuration!");

        File db = new File (param);
        try {
            mesh = new MeshDb (db);
        }
        catch (IOException ex) {
            Logger.error("Can't initialize mesh database", ex);
            throw new RuntimeException
                ("Can't initialize MeSH database: "+db);
        }
        
        lifecycle.addStopHook(() -> {
                wsclient.close();
                mesh.close();
                return F.Promise.pure(null);
            });
        
        Logger.debug("$"+ksp.getId()+": "+ksp.getName()
                     +" initialized; provider is "+ksp.getImplClass());
    }

    public MeshDb getMeshDb () {
        return mesh;
    }
    
    public void execute (KGraph kgraph, KNode... nodes) {
        Logger.debug("$"+ksp.getId()
                     +": executing on KGraph "+kgraph.getId()
                     +" \""+kgraph.getName()+"\"");
    }    
}
