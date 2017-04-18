package blackboard.pharos;

import java.util.concurrent.*;
import javax.inject.Inject;
import javax.inject.Named;
import play.Logger;
import play.Configuration;
import play.libs.ws.*;
import akka.actor.ActorSystem;

import blackboard.KSource;
import blackboard.KGraph;
import blackboard.KSourceProvider;

public class PharosKSource implements KSource {
    private final ActorSystem actorSystem;
    private final WSAPI wsapi;
    private final KSourceProvider ksp;
    
    @Inject
    public PharosKSource (ActorSystem actorSystem, WSAPI wsapi,
                          @Named("pharos") KSourceProvider ksp) {
        this.actorSystem = actorSystem;
        this.wsapi = wsapi;
        this.ksp = ksp;
        Logger.debug("$$ "+getClass().getName()
                     +" initialized; provider is "+ksp);
    }

    public void execute (KGraph kgraph) {
        if (kgraph != null)
            Logger.debug(getClass().getName()
                         +": executing on KGraph "+kgraph.getId()
                         +" "+kgraph.getName());
        else
            Logger.debug("Nothing to execute!");
        
        WSRequest req = wsapi.url(ksp.getUri()+"/targets/2");
        try {
            WSResponse res = req.get().toCompletableFuture().get();
            Logger.debug(">>>"+res.asJson());
        }
        catch (Exception ex) {
            Logger.error("Can't get response", ex);
        }       
    }
}
