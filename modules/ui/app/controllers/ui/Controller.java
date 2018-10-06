package controllers.ui;

import play.Configuration;
import play.mvc.*;
import play.Logger;
import play.libs.concurrent.HttpExecutionContext;

import javax.inject.Inject;

public class Controller extends play.mvc.Controller {
    final HttpExecutionContext ec;
    
    @Inject
    public Controller (HttpExecutionContext ec) {
        this.ec = ec;
    }
}
