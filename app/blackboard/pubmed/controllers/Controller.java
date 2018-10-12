
package blackboard.pubmed.controllers;

import javax.inject.Inject;
import java.util.concurrent.CompletionStage;

import play.Configuration;
import play.mvc.*;
import play.libs.ws.WSResponse;
import play.Logger;
import play.Environment;
import play.libs.Json;
import play.libs.concurrent.HttpExecutionContext;
import play.routing.JavaScriptReverseRouter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;


public class Controller extends play.mvc.Controller {
    final HttpExecutionContext ec;

    @Inject
    public Controller (HttpExecutionContext ec) {
        this.ec = ec;
    }

    public Result index () {
        return ok (blackboard.pubmed.views.html.index.render());
    }
}
