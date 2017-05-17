package blackboard.beacons;

import java.util.*;
import java.util.function.BiConsumer;
import java.net.URLEncoder;
import java.util.concurrent.*;

import javax.inject.Inject;
import javax.inject.Named;

import play.Logger;
import play.libs.ws.*;
import play.libs.Json;
import play.inject.ApplicationLifecycle;
import play.libs.F;

import com.fasterxml.jackson.databind.JsonNode;

import blackboard.*;
import static blackboard.KEntity.*;

public abstract class BeaconKSource implements KSource {
    protected final KSourceProvider ksp;    
    @Inject protected WSClient wsclient;
    
    @Inject
    public BeaconKSource (KSourceProvider ksp) {
        this.ksp = ksp;
        Logger.debug("$"+ksp.getId()+": "+ksp.getName()
                     +" initialized; provider is "+ksp.getImplClass());
    }

    @Inject
    protected void setLifecycle (ApplicationLifecycle lifecycle) {
        lifecycle.addStopHook(() -> {
                wsclient.close();
                return F.Promise.pure(null);
            });
        Logger.debug("lifecycle hook registered!");
    }
    
    protected void seedQuery (String term, KNode kn, KGraph kg) {
        String url = ksp.getUri()+"/concepts";
        WSRequest req = wsclient.url(url)
            .setFollowRedirects(true)
            .setQueryParameter("keywords", "\""+term+"\"");
        try {   
            WSResponse res = req.get().toCompletableFuture().get();
            JsonNode json = res.asJson();
            resolve (json, kn, kg);
        }
        catch (Exception ex) {
            Logger.error("Can't resolve url: "+url, ex);
        }
    }

    public void execute (KGraph kgraph, KNode... nodes) {
        Logger.debug("$"+ksp.getId()
                     +": executing on KGraph "+kgraph.getId()
                     +" \""+kgraph.getName()+"\"");
        if (nodes == null || nodes.length == 0)
            nodes = kgraph.getNodes();

        for (KNode kn : nodes) {
            switch (kn.getType()) {
            case "query":
            case "disease":
                seedQuery ((String)kn.get("term"), kn, kgraph);
            break;
            }
        }
    }

    protected abstract void resolve (JsonNode json, KNode kn, KGraph kgraph);
}
