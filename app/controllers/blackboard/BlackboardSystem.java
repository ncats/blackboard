package controllers.blackboard;

import javax.inject.*;
import play.*;
import play.mvc.*;
import play.libs.ws.*;
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

import services.*;

@Singleton
public class BlackboardSystem extends Controller {
    private ActorSystem actorSystem;
    private ExecutionContextExecutor exec;
    private Blackboard blackboard;
    private final ObjectMapper mapper = new ObjectMapper ();

    @Inject
    public BlackboardSystem (ActorSystem actorSystem,
                             ExecutionContextExecutor exec,
                             Blackboard blackboard) {
      this.actorSystem = actorSystem;
      this.exec = exec;
      this.blackboard = blackboard;
    }

    public Result listKG () {
        ArrayNode nodes = mapper.createArrayNode();
        for (KGraph kg : blackboard) {
            nodes.add(mapper.valueToTree(kg));
        }
        return ok (nodes);
    }

    @BodyParser.Of(value = BodyParser.Json.class)    
    public Result createKGraph () {
        JsonNode json = request().body().asJson();
        if (!json.hasNonNull("type")) 
            return badRequest ("Json has not \"type\" field!");

        String type = json.get("type").asText();
        if (!"query".equals(type))
            return badRequest
                ("Can't create knowledge graph with type \""+type+"\"");
        
        String name = json.hasNonNull("name")
            ? json.get("name").asText() : null;
        Map<String, Object> props = new TreeMap<>();
        for (Iterator<String> it = json.fieldNames(); it.hasNext(); ) {
            String field = it.next();
            props.put(field, json.get(field).asText());
        }
        KGraph kg = blackboard.createKGraph(name, props);
        
        return ok ((JsonNode)mapper.valueToTree(kg));
    }

    public Result getKG (Long id) {
        return ok ("KG "+id);
    }

    public Result getNodeTypes () {
        return ok ((JsonNode)mapper.valueToTree(blackboard.getNodeTypes()));
    }
    public Result getEdgeTypes () {
        return ok ((JsonNode)mapper.valueToTree(blackboard.getEdgeTypes()));
    }
    public Result getEvidenceTypes () {
        return ok ((JsonNode)mapper.valueToTree(blackboard.getEvidenceTypes()));
    }
}
