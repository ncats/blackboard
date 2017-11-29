
package blackboard.chembl;

import blackboard.KGraph;
import blackboard.KNode;
import blackboard.KSource;
import blackboard.KSourceProvider;
import javax.inject.Inject;
import javax.inject.Named;


import akka.actor.ActorSystem;
import play.Logger;
import play.libs.ws.*;
import play.inject.ApplicationLifecycle;
import play.libs.F;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.*;

import static blackboard.KEntity.*;

/**
 * Created by williamsmard on 11/27/17.
 */
public class ChemblKSource implements KSource{

    protected final KSourceProvider ksp;
    @Inject protected WSClient wsclient;

    interface Resolver {
        void resolve (JsonNode json, KNode kn, KGraph kg);
    }

    @Inject
    public ChemblKSource (ActorSystem actorSystem, WSClient wsclient,
                          @Named("chembl") KSourceProvider ksp,
                          ApplicationLifecycle lifecycle) {
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

    @Override
    public void execute(KGraph kgraph, KNode... nodes) {
        Logger.debug("$"+ksp.getId()
                +": executing on KGraph "+kgraph.getId()
                +" \""+kgraph.getName()+"\"");
        if (nodes == null || nodes.length == 0)
            nodes = kgraph.getNodes();

        for (KNode kn : nodes) {
            String url = (String)kn.get("uri");
            if(url!=null)
            {
                WSRequest req = wsclient.url(url)
                        .setFollowRedirects(true);
                try {
                    WSResponse res = req.get().toCompletableFuture().get();
                    JsonNode json = res.asJson();
                    Map aliasMap = getAliasMap(json);
                    aliasMap.keySet().forEach(key->{
                        if(key.toString().startsWith("CHEMBL"))
                        {
                            seedTarget((String)aliasMap.get(key),kn,kgraph);
                        }
                    });


                }
                catch (Exception ex) {
                    Logger.error("Can't resolve url: "+url, ex);
                }
            }
            else
            {
                System.out.println("url is null");
            }
        }
    }
    protected Map<String,String> getAliasMap(JsonNode json)
    {
        Map<String,String> aliasMap = new HashMap<>();
        try{
            if (json.isArray()) {
                for (int i = 0; i < json.size(); ++i)
                {
                    aliasMap.putAll(getAliasMap(json.get(i)));
                }
            }
            else{
                json.get("aliases").forEach(alias->{
                    if(alias!=null && alias.toString().contains(":"))
                    {
                        String[] test = alias.toString().replace("\"","").split(":");
                        aliasMap.put(test[0],test[1]);
                    }
                });
            }
        }
       catch (Exception ex)
       {
           Logger.error("Can't get aliases: "+ex);
       }
       return aliasMap;
    }
    protected void seedTarget(String id, KNode kn, KGraph kg) {
        Logger.debug("seedTarget");
        String url = ksp.getUri()+"/data/target";
        try {
            Map<String, String> q = new HashMap<>();
            q.put("molecule_chembl_id","CHEMBL"+id);
            q.put("format","json");
            resolve(url,q,kn,kg,this::resolveTarget);
        }
        catch (Exception ex) {
            Logger.error("Can't resolve url: "+url, ex);
        }
    }
    void resolve (String url, Map<String, String> params,
                  KNode kn, KGraph kg, Resolver resolver) {
        WSRequest req = wsclient.url(url).setFollowRedirects(true);
        if (params != null) {
            for (Map.Entry<String, String> me : params.entrySet()) {
                Logger.debug(".."+me.getKey()+": "+me.getValue());
                req = req.setQueryParameter(me.getKey(), me.getValue());
                Logger.debug(me.getKey()+", "+me.getValue());
            }
        }
        Logger.debug("+++ resolving..."+req.getUrl());

        try {
            WSResponse res = req.get().toCompletableFuture().get();
            JsonNode json = res.asJson();
            resolver.resolve(json, kn, kg);
        }
        catch (Exception ex) {
            Logger.error("Can't resolve url: "+url, ex);
        }
    }

    void resolveTarget(JsonNode json, KNode kn, KGraph kg) {
        Logger.debug("resolveTarget");
        if (json.isArray()) {
            for (int i = 0; i < json.size(); ++i)
                resolveTarget(json.get(i), kn, kg);
        }
        else {
            Map<String, Object> props = new TreeMap<>();
            JsonNode targets = json.get("targets");
            if(targets.isArray())
            {
                for (int i = 0; i < targets.size(); ++i)
                {
                    JsonNode currentTarget = targets.get(i);
                    props.put(TYPE_P,"protein");
                    props.put(NAME_P,currentTarget.get("pref_name").asText());
                    KNode xn = kg.createNodeIfAbsent(props, URI_P);
                    if (xn.getId() != kn.getId()) {
                        xn.addTag("KS:"+ksp.getId());
                        kg.createEdgeIfAbsent(kn, xn, "resolve");
                    }
                }
            }

        }
    }
}

