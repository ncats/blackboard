package blackboard.pharos;

import java.util.concurrent.*;
import javax.inject.Inject;
import play.Logger;
import play.Configuration;
import play.libs.ws.*;
import akka.actor.ActorSystem;

import blackboard.KSource;
import blackboard.KGraph;

public class PharosKSource implements KSource {
    private final ActorSystem actorSystem;
    private final WSAPI wsapi;
    
    @Inject
    public PharosKSource (ActorSystem actorSystem, WSAPI wsapi) {
        this.actorSystem = actorSystem;
        this.wsapi = wsapi;
        WSRequest req = wsapi.url
            ("https://pharos.ncats.io/idg/api/v1/targets/2");
        try {
            WSResponse res = req.get().toCompletableFuture().get();
            Logger.debug(">>>"+res.asJson());
        }
        catch (Exception ex) {
            Logger.error("Can't get response", ex);
        }
        Logger.debug("$$ "+getClass().getName()+" initialized!");
    }

    public void execute (KGraph kgraph) {
        Logger.debug(getClass().getName()
                     +": executing on KGraph "+kgraph.getId()
                     +" "+kgraph.getName());
    }
}
