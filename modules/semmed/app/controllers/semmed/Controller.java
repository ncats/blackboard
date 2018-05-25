package controllers.semmed;

import java.util.*;

import javax.inject.Inject;
import javax.inject.Singleton;
import play.Configuration;
import play.mvc.*;
import play.libs.ws.WSResponse;
import play.Logger;
import play.libs.Json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import blackboard.semmed.SemMedDbKSource;


@Singleton
public class Controller extends play.mvc.Controller {
    final SemMedDbKSource ks;
    
    @Inject
    public Controller (Configuration config, SemMedDbKSource ks) {
        this.ks = ks;
    }

    public Result index () {
        return ok ("This is a basic implementation of the "
                   +"SemMedDB knowledge source!");
    }
}
