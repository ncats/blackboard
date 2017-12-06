
package blackboard.chembl;

import blackboard.KGraph;
import blackboard.KNode;
import blackboard.KSource;
import blackboard.KSourceProvider;
import javax.inject.Inject;
import javax.inject.Named;


import akka.actor.ActorSystem;
import play.Logger;
import play.libs.Json;
import play.libs.ws.*;
import play.inject.ApplicationLifecycle;
import play.libs.F;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.*;
import java.util.concurrent.ExecutionException;

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
                            seedDrug("CHEMBL"+(String)aliasMap.get(key),kn,kgraph);
                            seedTarget("CHEMBL"+(String)aliasMap.get(key),kn,kgraph);
                            seedMechanism("CHEMBL"+(String)aliasMap.get(key),kn,kgraph);
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
                if(kn.get("synonyms")!=null)
                {
                    String synonyms = kn.get("synonyms").toString();
                    if(synonyms.startsWith("CHEMBL"))
                    {
                        seedDrug(synonyms,kn,kgraph);
                        seedTarget(synonyms,kn,kgraph);
                        seedMechanism(synonyms,kn,kgraph);
                    }
                }
            }
        }
    }
    protected void seedMechanism(String id,KNode kn,KGraph kg)
    {
        String url = ksp.getUri()+"/data/mechanism.json";
        try
        {
            Map<String,String>q=new HashMap<>();
            q.put("molecule_chembl_id",id);
            resolve(url,q,kn,kg,this::resolveMechanism);
        }
        catch(Exception ex)
        {
            Logger.debug(ex.getMessage());
        }
    }
    protected void resolveMechanism(JsonNode json,KNode kn, KGraph kg)
    {
        if (json.isArray()) {
            for (int i = 0; i < json.size(); ++i)
                resolveMechanism(json.get(i), kn, kg);
        }
        else {
            Map<String, Object> props = new TreeMap<>();
            JsonNode mechanisms = json.get("mechanisms");
            if(mechanisms.isArray())
            {
                for(int i=0;i<mechanisms.size();++i)
                {
                    JsonNode currentMechanism = mechanisms.get(i);
                    String moa = currentMechanism.get("mechanism_of_action").toString();
                    String targetId=currentMechanism.get("target_chembl_id").toString();
                    String targetName = moa.substring(0,moa.lastIndexOf(" "));
                    String action = currentMechanism.get("action_type").toString();
                    if(!kn.hasTag("mechanism"))
                    {
                        kn.put("mechanism",unquote(moa));

                    }
                    else
                    {
                        String mech = kn.get("mechanism").toString();
                        kn.put("mechanism",mech+","+unquote(moa));
                    }
                    System.out.println("moa "+moa);
                    props.put(TYPE_P,"protein");
                    props.put(NAME_P,unquote(targetName));
                    props.put(SYNONYMS_P,unquote(targetId));
                    KNode xn = kg.createNodeIfAbsent(props, URI_P);
                    if (xn.getId() != kn.getId()) {
                        xn.addTag("KS:"+ksp.getId());
                        kg.createEdgeIfAbsent(kn, xn, unquote(action));
                    }
                }
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
            q.put("molecule_chembl_id",id);
            q.put("format","json");
            resolve(url,q,kn,kg,this::resolveTarget);
        }
        catch (Exception ex) {
            Logger.error("Can't resolve url: "+url, ex);
        }
    }
    void resolveDrug(JsonNode json, KNode kn, KGraph kg)
    {
        if (json.isArray()) {
            for (int i = 0; i < json.size(); ++i)
                resolveDrug(json.get(i), kn, kg);
        }
        else {
            Map<String, Object> props = new TreeMap<>();
            JsonNode forms = json.get("forms");
            if(forms.isArray())
            {
                for(int i = 0; i< forms.size();++i)
                {
                    JsonNode currentDrug = forms.get(i);
                    String chemblId = currentDrug.get("chemblId").toString();
                    String url = ksp.getUri()+"/data/molecule.json";
                    WSRequest req = wsclient.url(url).setFollowRedirects(true);
                    req.setQueryParameter("molecule_chembl_id",chemblId.replace("\"",""));
                    System.out.println(req.getUrl());
                    try{
                        WSResponse res = req.get().toCompletableFuture().get();
                        JsonNode molNode = res.asJson();
                        JsonNode drugsNode = molNode.get("molecules");
                        if(drugsNode.isArray())
                        {
                            for(int j=0;j<drugsNode.size();j++)
                            {
                                JsonNode currentNode = drugsNode.get(j);
                                if(!currentNode.get("pref_name").toString().equals("null")){
                                    props.put(NAME_P,unquote(currentNode.get("pref_name").toString()));
                                    props.put(TYPE_P,"drug");
                                    props.put(SYNONYMS_P,unquote(chemblId));
                                    props.put(URI_P,"https://www.ebi.ac.uk/chembl/compound/inspect/"+chemblId);
                                    KNode xn = kg.createNodeIfAbsent(props, URI_P);
                                    if (xn.getId() != kn.getId()) {
                                        xn.addTag("KS:"+ksp.getId());
                                        kg.createEdgeIfAbsent(kn, xn, "is equivalent to");
                                    }
                                }
                            }
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }
    public String prettyPrint (JsonNode json)
    {
        StringBuilder sb = new StringBuilder();
        Iterator<String> i = json.fieldNames();
        while(i.hasNext())
        {
            String next = i.next();
            sb.append(next);
            sb.append(": ");
            sb.append(json.get(next));
            sb.append("\n");
        }
        return sb.toString();
    }
    public String unquote (String s)
    {
        return s.replace("\"","");
    }
    protected void seedDrug(String id, KNode kn, KGraph kg)
    {
        Logger.debug("seedDrug");
        String url = "https://www.ebi.ac.uk/chemblws/compounds/"+id+"/form.json";
        System.out.println("URL "+url);
        try{
            Map<String, String> q = new HashMap<>();
            resolve(url,q,kn,kg,this::resolveDrug);
        }
        catch(Exception ex)
        {
            Logger.error("can't resolve url: "+url,ex);
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
                    String chemblId = currentTarget.get("target_chembl_id").toString();
                    props.put(TYPE_P,"protein");
                    props.put(NAME_P,currentTarget.get("pref_name").asText());
                    props.put(URI_P,"https://www.ebi.ac.uk/chembl/target/inspect/"+unquote(chemblId));
                    KNode xn = kg.createNodeIfAbsent(props, URI_P);
                    if (xn.getId() != kn.getId()) {
                        xn.addTag("KS:"+ksp.getId());
                        kg.createEdgeIfAbsent(kn, xn, "targets");
                    }
                }
            }

        }
    }

}

