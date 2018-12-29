package blackboard.aws;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.comprehendmedical.AWSComprehendMedicalClientBuilder;
import com.amazonaws.services.comprehendmedical.AWSComprehendMedical;
import com.amazonaws.services.comprehendmedical.AWSComprehendMedicalClient;
import com.amazonaws.services.comprehendmedical.model.DetectEntitiesRequest;
import com.amazonaws.services.comprehendmedical.model.DetectEntitiesResult;

import play.Logger;
import play.libs.Json;
import play.libs.ws.*;
import play.inject.ApplicationLifecycle;
import play.libs.F;
import play.cache.*;

import blackboard.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

@Singleton
public class AWSKSource implements KSource {
    final AWSComprehendMedical comprehendmedical;

    @Inject
    public AWSKSource (@Named("aws") KSourceProvider ksp,
                       ApplicationLifecycle lifecycle) {
        AWSComprehendMedical acm = null;
        try {
            acm = init (ksp, lifecycle);
        }
        catch (Exception ex) {
            Logger.error("Can't initialize AWS resource!", ex);
        }
        comprehendmedical = acm;
    }

    static AWSComprehendMedical init (KSourceProvider ksp,
                                      ApplicationLifecycle lifecycle)
        throws Exception {
        AWSComprehendMedical comprehendmedical;
        
        AWSComprehendMedicalClientBuilder builder =
            AWSComprehendMedicalClient.builder();
            
        Map<String, String> props = ksp.getProperties();
        String id = props.get("access-key-id");
        String secret = props.get("secret-access-key");
            
        if (id == null || "".equals(id)
            || secret == null || "".equals(secret)) {
            Logger.debug("## Using AWS default client!");
            comprehendmedical = builder.defaultClient();
        }
        else {
            final BasicAWSCredentials awsCreds =
                new BasicAWSCredentials (id, secret);
            builder = builder.standard()
                .withCredentials(new AWSStaticCredentialsProvider (awsCreds));
            String region = props.get("region");
            if (region != null || "".equals(region))
                builder = builder.withRegion("us-east-1");
            comprehendmedical = builder.build();
        }

        lifecycle.addStopHook(() -> {
                comprehendmedical.shutdown();
                return CompletableFuture.completedFuture(null);
            });
        
        Logger.debug("$"+ksp.getId()+": "+ksp.getName()
                     +" initialized; provider is "+ksp.getImplClass());
        return comprehendmedical;
    }        

    public JsonNode comprehendMedical (String text) {
        if (comprehendmedical != null) {
            DetectEntitiesRequest request =
                new DetectEntitiesRequest().withText(text);
            DetectEntitiesResult result =
                comprehendmedical.detectEntities(request);
            ObjectNode obj = (ObjectNode) Json.toJson(result);
            obj.put("text", text);
            return obj;
        }
        Logger.error("AWS knowledge source has not been properly initialized!");
        return null;
    }

    /*
     * KSource interface
     */
    public void execute (KGraph kgraph, KNode... nodes) {
    }
}
