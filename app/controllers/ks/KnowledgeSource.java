package controllers.ks;

import javax.inject.*;
import play.*;
import play.inject.Injector;
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

import controllers.blackboard.BlackboardSystem;
import blackboard.KSource;

@Singleton
public class KnowledgeSource extends Controller {
    @Inject ActorSystem actorSystem;
    @Inject BlackboardSystem bbsys;
    @Inject @Named("pharos") KSource pharos;
    
    public KnowledgeSource () {
    }
    
    public Result index () {
        return ok ();
    }
}
