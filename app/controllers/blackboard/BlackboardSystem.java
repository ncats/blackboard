package controllers.blackboard;

import javax.inject.*;
import play.*;
import play.mvc.*;
import play.libs.ws.*;
import play.libs.Json;
import static play.mvc.Http.MultipartFormData.*;

import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import scala.concurrent.duration.Duration;
import scala.concurrent.ExecutionContextExecutor;
import akka.actor.ActorSystem;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import blackboard.*;
import static blackboard.KEntity.*;
import controllers.ks.KnowledgeSource;

@Singleton
public class BlackboardSystem extends Controller {
    private final ActorSystem actorSystem;
    private final Blackboard blackboard;
    private final KnowledgeSource knowledgeSource;

    @Inject
    public BlackboardSystem (ActorSystem actorSystem,
                             JsonCodec codec,
                             KnowledgeSource knowledgeSource,
                             Blackboard blackboard) {
      this.actorSystem = actorSystem;
      this.blackboard = blackboard;
      this.knowledgeSource = knowledgeSource;
      Json.setObjectMapper(codec.getObjectMapper());
    }

    public Result listKG () {
        ArrayNode nodes = Json.newArray();
        for (KGraph kg : blackboard) {
            nodes.add(Json.toJson(kg));
        }
        return ok (nodes);
    }

    @BodyParser.Of(value = BodyParser.Json.class)    
    public Result createKGraph () {
        JsonNode json = request().body().asJson();
        if (!json.hasNonNull(TYPE_P)) 
            return badRequest ("Json has not \""+TYPE_P+"\" field!");

        String type = json.get(TYPE_P).asText();
        if (!"query".equals(type))
            return badRequest
                ("Can't create knowledge graph with type \""+type+"\"");
        
        Map<String, Object> props = new TreeMap<>();
        for (Iterator<String> it = json.fieldNames(); it.hasNext(); ) {
            String field = it.next();
            props.put(field, json.get(field).asText());
        }
        
        KGraph kg = blackboard.createKGraph(props);
        return ok (Json.toJson(kg));
    }

    public Result getKG (Long id) {
        KGraph kg = blackboard.getKGraph(id);
        if (kg == null)
            return notFound ("Unknown knowledge graph: "+id);
        
        return ok (Json.toJson(kg));
    }

    public Result runKS (Long id, String ks) {
        KGraph kg = blackboard.getKGraph(id);
        if (kg == null)
            return badRequest ("Unknown knowledge graph requested: "+id);

        try {
            knowledgeSource.runKS(ks, kg);
        }
        catch (Exception ex) {
            ex.printStackTrace();
            return badRequest (ex.getMessage());
        }
        return ok ("Knowledge source \""+ks
                   +"\" successfully executed on knowledge graph "+id);
    }

    public Result getNodesForKG (Long id) {
        KGraph kg = blackboard.getKGraph(id);
        if (kg == null)
            return badRequest ("Unknown knowledge graph requested: "+id);
        return ok (Json.toJson(kg.getNodes()));
    }

    public Result getNodeForKG (Long id, Long nid) {
        KGraph kg = blackboard.getKGraph(id);
        if (kg == null)
            return badRequest ("Unknown knowledge graph requested: "+id);
        KNode kn = kg.node(nid);
        if (kn == null)
            return badRequest ("Knowledge graph "+id+" has no node: "+nid);
        return ok (Json.toJson(kn));
    }

    public Result getEdgesForKG (Long id) {
        KGraph kg = blackboard.getKGraph(id);
        if (kg == null)
            return badRequest ("Unknown knowledge graph requested: "+id);
        return ok (Json.toJson(kg.getEdges()));
    }

    public Result getEdgeForKG (Long id, Long eid) {
        KGraph kg = blackboard.getKGraph(id);
        if (kg == null)
            return badRequest ("Unknown knowledge graph requested: "+id);
        KEdge ke = kg.edge(eid);
        if (ke == null)
            return badRequest ("Knowledge graph "+id+" has no edge: "+eid);
        return ok (Json.toJson(ke));
    }
    
    public Result getNodeTypes () {
        return ok (Json.toJson(blackboard.getNodeTypes()));
    }
    public Result getEdgeTypes () {
        return ok (Json.toJson(blackboard.getEdgeTypes()));
    }
    public Result getEvidenceTypes () {
        return ok (Json.toJson(blackboard.getEvidenceTypes()));
    }
}
