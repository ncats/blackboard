package ks;

import play.Logger;
import play.inject.Injector;
import javax.inject.Inject;
import javax.inject.Provider;

import blackboard.KSource;

public class KSourceProvider implements Provider<KSource> {
    @Inject
    private Injector injector;
    private KSource ksource;
    
    public String name;
    public String version;
    public String uri;
    public final String klass;

    public KSourceProvider (String klass) {
        if (klass == null)
            throw new IllegalArgumentException ("KSource class is null");
        this.klass = klass;
    }

    public KSource get () {
        if (ksource == null) {
            try {
                ksource = (KSource) injector.instanceOf(Class.forName(klass));
            }
            catch (Exception ex) {
                Logger.error("Unknown KSource class: "+klass, ex);
            }
        }
        return ksource;
    }
}
