package controllers.blackboard;

import javax.inject.*;
import play.*;
import play.mvc.*;
import play.libs.ws.*;
import static play.mvc.Http.MultipartFormData.*;

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
        }
        return ok (nodes);
    }

    @BodyParser.Of(value = BodyParser.Json.class)    
    public Result createKG () {
        JsonNode json = request().body().asJson();
        return ok (json);
    }

    public Result getKG (Long id) {
        return ok ("KG "+id);
    }

    public Result getTypes () {
        return ok ((JsonNode)mapper.valueToTree(blackboard.getTypes()));
    }
}
