package controllers.umls;

import java.util.*;

import javax.inject.Inject;
import javax.inject.Singleton;
import play.Configuration;
import play.mvc.*;
import play.libs.ws.WSResponse;
import play.Logger;
import play.libs.Json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import blackboard.umls.UMLSKSource;

@Singleton
public class Controller extends play.mvc.Controller {
    final UMLSKSource ks;
    
    @Inject
    public Controller (Configuration config, UMLSKSource ks) {
        this.ks = ks;
    }

    public Result index () {
        return ok
            ("This is a basic implementation of the UMLS knowledge source!");
    }

    public Result ticket () {
        try {
            return ok (ks.ticket());
        }
        catch (Exception ex) {
            return internalServerError (ex.getMessage());
        }
    }

    static ObjectNode toObject (JsonNode node) {
        Map<String, JsonNode> fields = new HashMap<>();
        for (Iterator<Map.Entry<String, JsonNode>> it =
                 node.fields(); it.hasNext();) {
            Map.Entry<String, JsonNode> me = it.next();
            fields.put(me.getKey(), me.getValue());
        }
        ObjectNode n = Json.newObject();
        n.setAll(fields);
        return n;
    }
    
    static ObjectNode instrument (JsonNode node) {
        ObjectNode n = toObject (node);
        String cui = n.get("ui").asText();
        n.put("_cui", controllers.umls.routes.Controller.cui(cui).url());
        n.put("_atoms", routes.Controller.atoms(cui).url());
        n.put("_definitions", routes.Controller.definitions(cui).url());
        n.put("_relations", routes.Controller.relations(cui).url());
        
        return n;
    }

    public Result search (String q, Integer top, Integer skip) {
        try {
            WSResponse res = ks.search(q)
                .setQueryParameter("pageSize", top.toString())
                .setQueryParameter("pageNumber", String.valueOf(skip/top+1))
                .get().toCompletableFuture().get();
            JsonNode json = res.asJson();
            if (json.hasNonNull("result")) {
                JsonNode results = json.get("result").get("results");
                ArrayNode ary = Json.newArray();
                for (int i = 0; i < results.size(); ++i) {
                    JsonNode r = results.get(i);
                    ary.add(instrument (r));
                }
                json = ary;
            }
            else
                json = Json.newArray();
            return status (res.getStatus(), json);
        }
        catch (Exception ex) {
            Logger.error("Can't process search \""+q+"\"", ex);
            return internalServerError (ex.getMessage());
        }
    }

    public Result cui (String cui) {
        try {
            JsonNode json = ks.getCui(cui);
            return json != null ? ok (json)
                : notFound("Request not matched: "+request().uri());
        }
        catch (Exception ex) {
            Logger.error("Can't retrieve "+cui, ex);
            return internalServerError (ex.getMessage());
        }
    }

    public Result full (String cui) {
        try {
            JsonNode json = ks.getCui(cui);
            if (json != null) {
                ObjectNode n = toObject (json);
                n.put("definitions", ks.getContent(cui, "definitions"));
                n.put("atoms", ks.getContent(cui, "atoms"));
                n.put("relations", ks.getContent(cui, "relations"));
                json = n;
            }
                
            return json != null ? ok (json)
                : notFound("Request not matched: "+request().uri());
        }
        catch (Exception ex) {
            Logger.error("Can't retrieve "+cui, ex);
            return internalServerError (ex.getMessage());
        }
    }

    Result content (String cui, String context) {
        try {
            JsonNode json = ks.getContent(cui, context);
            return json != null ? ok (json)
                : notFound("Request not matched: "+request().uri());
        }
        catch (Exception ex) {
            return internalServerError (ex.getMessage());
        }
    }

    public Result atoms (String cui) {
        return content (cui, "atoms");
    }
    
    public Result definitions (String cui) {
        return content (cui, "definitions");
    }
    
    public Result relations (String cui) {
        return content (cui, "relations");
    }

    public Result source (String src, String id) {
        try {
            WSResponse res = ks.source(src, id, "atoms/preferred")
                .get().toCompletableFuture().get();
            JsonNode json = res.asJson();
            if (json.hasNonNull("result")) {
                return status (res.getStatus(), json.get("result"));
            }
            return notFound ("Request not matched: "+request().uri());
        }
        catch (Exception ex) {
            Logger.error("Can't retrieve "+id+" for source "+src, ex);
            return internalServerError (ex.getMessage());
        }
    }
}

