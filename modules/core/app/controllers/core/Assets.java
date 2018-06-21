package controllers.core;

import play.api.mvc.*;
import play.api.http.HttpErrorHandler;
import javax.inject.Inject;
import play.Logger;

public class Assets extends controllers.AssetsBuilder {
    @Inject
    Assets (HttpErrorHandler eh) {
        super (eh);
    }

    public Action<AnyContent> versioned
        (String path, controllers.Assets.Asset file) {
        /*
        // why do i have to do this?
        if (file.toString().startsWith("Asset(lib/")) {
            path = "/public";
        }
        Logger.debug("path="+path+" file="+file);
        */
        return super.versioned(path, file);
    }
}
