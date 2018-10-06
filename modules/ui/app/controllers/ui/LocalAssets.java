package controllers.ui;

import play.api.mvc.*;
import play.api.http.HttpErrorHandler;
import javax.inject.Inject;
import play.Logger;
import static controllers.Assets.Asset;

public class LocalAssets extends controllers.AssetsBuilder {
    @Inject
    LocalAssets (HttpErrorHandler eh, controllers.AssetsMetadata meta) {
        super (eh, meta);
    }

    public Action<AnyContent> versioned (String path, Asset file) {
        return super.versioned(path, file);
    }
}
