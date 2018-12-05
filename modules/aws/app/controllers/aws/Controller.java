package controllers.aws;

import java.util.*;
import java.util.regex.*;
import java.util.concurrent.CompletionStage;
import static java.util.concurrent.CompletableFuture.supplyAsync;

import javax.inject.Inject;
import javax.inject.Singleton;
import play.Configuration;
import play.mvc.*;
import play.libs.ws.WSResponse;
import play.Logger;
import play.libs.Json;
import play.libs.concurrent.HttpExecutionContext;
import play.routing.JavaScriptReverseRouter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.core.JsonParseException;

import blackboard.aws.AWSKSource;

@Singleton
public class Controller extends play.mvc.Controller {
    final AWSKSource aws;
    final HttpExecutionContext ec;
    
    @Inject
    public Controller (HttpExecutionContext ec, AWSKSource aws) {
        this.ec = ec;
        this.aws = aws;
    }

    public Result index () {
        return ok (aws.getClass().getName());
    }

    public CompletionStage<Result> apiComprehendMedical (String text) {
        return supplyAsync (() -> {
                try {
                    return ok (aws.comprehendMedical(text));
                }
                catch (Exception ex) {
                    Logger.error("apiComprehendMedical: "+ex.getMessage(), ex);
                    return internalServerError (ex.getMessage());
                }
            }, ec.current());
    }

    @BodyParser.Of(value = BodyParser.AnyContent.class)
    public CompletionStage<Result> apiComprehendMedicalPost () {
        return supplyAsync (() -> {
                try {
                    String text = request().body().asText();
                    if (text != null) {
                        return ok (aws.comprehendMedical(text));
                    }
                    return badRequest ("Invalid POST content type!");
                }
                catch (Exception ex) {
                    Logger.error("apiComprehendMedical: "+ex.getMessage(), ex);
                    return internalServerError (ex.getMessage());
                }
            }, ec.current());
    }
}
