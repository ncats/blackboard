package controllers;

import javax.inject.Inject;
import javax.inject.Singleton;
import play.Configuration;
import play.mvc.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

@Singleton
public class PharosController extends Controller {
    @Inject
    public PharosController (Configuration config) {
    }

    public Result index () {
        return ok ("Information about Pharos knowledge source goes here!");
    }
}

