package controllers.core;

import play.api.mvc.*;
import play.api.http.HttpErrorHandler;
import javax.inject.Inject;
import play.Logger;
import static controllers.Assets.Asset;

public class CustomAssets extends controllers.AssetsBuilder {
    @Inject
    CustomAssets (HttpErrorHandler eh) {
        super (eh);
    }

    public Action<AnyContent> versioned (String path, Asset file) {
        return super.versioned(path, file);
    }
}
