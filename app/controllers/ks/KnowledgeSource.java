package controllers.ks;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.inject.Named;
import play.*;
import play.mvc.*;
import play.libs.Json;
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

import com.google.inject.Injector;
import com.google.inject.TypeLiteral;
import com.google.inject.Binding;

import com.fasterxml.jackson.databind.JsonNode;

import blackboard.KSource;
import blackboard.KSourceProvider;
import blackboard.KGraph;
import blackboard.KNode;

@Singleton
public class KnowledgeSource extends Controller {
    public static final TypeLiteral<KSourceProvider> KTYPE =
        new TypeLiteral<KSourceProvider>(){};
    
    final Injector injector;
    final ActorSystem actorSystem;

    final Map<String, KSourceProvider> ksources;

    @Inject
    public KnowledgeSource (Injector injector,
                            ActorSystem actorSystem) {
        this.injector = injector;
        this.actorSystem = actorSystem;

        ksources = new TreeMap<>();
        for (Binding<KSourceProvider> ksb
                 : injector.findBindingsByType(KTYPE)) {
            KSourceProvider ksp = ksb.getProvider().get();
            ksources.put(ksp.getId(), ksp);
        }
        Logger.debug(ksources.size()+" knowledge sources defined!");
    }
    
    public Result index () {
        return ok (Json.toJson(ksources));
    }

    public Result getKS (String ks) {
        KSourceProvider ksp = ksources.get(ks);
        if (ksp != null) {
            return ok (Json.toJson(ksp));
        }
        return notFound ("Unknown knowledge source: "+ks);
    }

    public void runKS (String ks, KGraph kgraph, KNode... nodes) {
        KSourceProvider ksp = ksources.get(ks);
        if (ksp == null)
            throw new IllegalArgumentException
                ("Unknown knowledge source \""+ks+"\"");
        ksp.getKS().execute(kgraph, nodes);
    }
}
