package controllers;

import javax.inject.Inject;
import javax.inject.Singleton;
import play.Configuration;
import play.mvc.*;
import play.libs.ws.WSResponse;
import play.Logger;
import play.libs.Json;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import blackboard.umls.UMLSKSource;

@Singleton
public class UMLSController extends Controller {
    final UMLSKSource ks;
    
    @Inject
    public UMLSController (Configuration config, UMLSKSource ks) {
        this.ks = ks;
    }

    public Result index () {
        return ok ("This is a basic implementation of the UMLS knowledge source!");
    }

    public Result ticket () {
        try {
            return ok (ks.ticket());
        }
        catch (Exception ex) {
            return internalServerError (ex.getMessage());
        }
    }

    public Result search (String q) {
        try {
            WSResponse res = ks.search(q).get().toCompletableFuture().get();
            return status (res.getStatus(), res.asJson());
        }
        catch (Exception ex) {
            return internalServerError (ex.getMessage());
        }
    }
}

