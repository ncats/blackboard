package blackboard.beacons;

import java.util.*;
import java.net.URLEncoder;
import java.util.concurrent.*;

import javax.inject.Inject;
import javax.inject.Named;

import play.Logger;
import play.libs.ws.*;
import play.libs.Json;
import play.inject.ApplicationLifecycle;
import play.libs.F;
import akka.actor.ActorSystem;

import com.fasterxml.jackson.databind.JsonNode;

import blackboard.*;
import static blackboard.KEntity.*;

public class MedGenInformaticsKSource extends BeaconKSource {
    @Inject
    public MedGenInformaticsKSource
        (@Named("beacons-medgen") KSourceProvider ksp) {
        super (ksp);
    }

    @Override
    protected void resolve (JsonNode json, KNode kn, KGraph kg) {
        if (json.isArray()) {
            for (int i = 0; i < json.size(); ++i)
                resolve (json.get(i), kn, kg);
        }
        else {
            Map<String, Object> props = new TreeMap<>();
            props.put(TYPE_P, "disease");
            props.put(URI_P, ksp.getUri()+"/concepts/"+json.get("id").asText());
            props.put(NAME_P, json.get("name").asText());
            List<String> syns = new ArrayList<>();
            JsonNode sn = json.get(SYNONYMS_P);
            if (sn != null) {
                for (int i = 0; i < sn.size(); ++i)
                    syns.add(sn.get(i).asText());
                props.put(SYNONYMS_P, syns.toArray(new String[0]));
            }
            KNode xn = kg.createNodeIfAbsent(props, URI_P);
            if (xn.getId() != kn.getId()) {
                xn.addTag("KS:"+ksp.getId());
                kg.createEdgeIfAbsent(kn, xn, "resolve");
            }
        }
    }
}
