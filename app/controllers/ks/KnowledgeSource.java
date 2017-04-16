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
    private ActorSystem actorSystem;
    private BlackboardSystem bbsys;
    private ArrayNode ksinfo;
    private Map<String, KSource> ksources;
    
    @Inject
    public KnowledgeSource (Configuration config,
                            Injector injector,
                            ActorSystem actorSystem,
                            BlackboardSystem bbsys) {
        ObjectMapper mapper = new ObjectMapper ();
        ksinfo = mapper.createArrayNode();
        ksources = new HashMap<>();
        
        List<Configuration> ksconfigs = config.getConfigList
            ("blackboard.ksources", new ArrayList<>());
        for (Configuration c : ksconfigs) {
            ObjectNode n = mapper.createObjectNode();
            String id = c.getString("id", null);
            if (id == null) {
                Logger.warn("Knowledge has no \"id\" field!");
            }
            else {
                n.put("id", id);
                n.put("name", c.getString("name", null));
                n.put("version", c.getString("version", null));
                n.put("uri", c.getString("uri", null));
                String cls = c.getString("class", null);
                if (cls != null) {
                    try {
                        KSource ks =
                            (KSource)injector.instanceOf(Class.forName(cls));
                        ksources.put(id, ks);
                        ksinfo.add(n);
                    }
                    catch (Exception ex) {
                        Logger.error("Can't instantiate knowledge source: "
                                     +id, ex);
                    }
                }
                else {
                    Logger.warn("Knowledge source \""
                                +id+"\" has no \"class\" specified!");
                }
            }
        }
        this.actorSystem = actorSystem;
        this.bbsys = bbsys;
    }
    
    public Result index () {
        return ok (ksinfo);
    }
}
