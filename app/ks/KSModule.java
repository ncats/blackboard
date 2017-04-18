package ks;

import java.util.Set;
import javax.inject.Inject;
import javax.inject.Provider;

import scala.collection.Seq;
import play.inject.NamedImpl;
import play.inject.Injector;
import play.Logger;
import play.api.Configuration;
import play.api.Environment;
import play.api.inject.Binding;
import play.api.inject.Module;
import play.libs.Scala;

import com.typesafe.config.Config;
import com.google.common.collect.ImmutableList;
import com.fasterxml.jackson.annotation.JsonIgnore;

import blackboard.KSourceProvider;
import blackboard.KSource;

public class KSModule extends Module {
    public static class KSourceProviderImpl
        implements KSourceProvider, Provider<KSourceProvider> {
        
        @Inject
        private Injector injector;
        private KSource ksource;

        public String id;
        public String name;
        public String version;
        public String uri;
        @JsonIgnore
        public final String klass;
        
        public KSourceProviderImpl (String klass) {
            if (klass == null)
                throw new IllegalArgumentException ("KSource class is null");
            this.klass = klass;
        }

        // lazy evaluation
        public KSource getKS () {
            if (ksource == null) {
                try {
                    ksource =
                        (KSource)injector.instanceOf(Class.forName(klass));
                }
                catch (Exception ex) {
                    Logger.error("Can't instantiate KSource instance: "
                                 +klass, ex);
                }
            }
            return ksource;
        }

        public String getId () { return id; }
        public String getName () { return name; }
        public String getVersion () { return version; }
        public String getUri () { return uri; }
        public String getImplClass () { return klass; }

        public KSourceProvider get () { return this; }
    }
    
    @Override
    public Seq<Binding<?>> bindings(Environment environment,
                                    Configuration configuration) {
        ImmutableList.Builder<Binding<?>> list =
            new ImmutableList.Builder<Binding<?>>();
        try {
            Set<String> kss = configuration.underlying()
                .getConfig("ksource").root().keySet();
            for (String ks : kss) {
                Config conf = configuration.underlying()
                    .getConfig("ksource."+ks);
                
                if (conf.hasPath("class")) {
                    String klass = conf.getString("class");
                    Logger.debug("binding \""+ks+"\".."+klass);
                    try {
                        KSourceProviderImpl ksp =
                            new KSourceProviderImpl (klass);
                        ksp.id = ks;
                        if (conf.hasPath("name"))
                            ksp.name = conf.getString("name");
                        if (conf.hasPath("version"))
                            ksp.version = conf.getString("version");
                        if (conf.hasPath("uri"))
                            ksp.uri = conf.getString("uri");
                        list.add(bind (KSourceProvider.class)
                                 .qualifiedWith(new NamedImpl (ks))
                                 .to(ksp));
                    }
                    catch (Exception ex) {
                        Logger.error("KSource \""+ks
                                     +"\" has bad class: "+klass, ex);
                    }
                }
                else {
                    Logger.warn("KSource \""+ks+"\" has no class defined!");
                }
            }
        }
        catch (com.typesafe.config.ConfigException.Missing ex) {
            Logger.warn("Bad configuration", ex);
        }

        return Scala.toSeq(list.build());
    }
}
