package ks;

import java.io.File;
import java.net.URL;
import java.util.Set;
import java.util.Map;
import java.util.TreeMap;
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
import play.libs.Json;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import com.google.common.collect.ImmutableList;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonNode;

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
        @JsonIgnore
        public final Map<String, String> properties = new TreeMap<>();
        public JsonNode data;
        
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
        public Map<String, String> getProperties () { return properties; }
        public KSourceProvider get () { return this; }
        public JsonNode getData () { return data; }
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
                        
                        for (Map.Entry<String, ConfigValue> me
                                 : conf.entrySet()) {
                            String val = me.getValue().unwrapped().toString();
                            switch (me.getKey()) {
                            case "name":
                                ksp.name = val;
                                break;
                            case "version":
                                ksp.version = val;
                                break;
                            case "uri":
                                ksp.uri = val;
                            default:
                                ksp.properties.put(me.getKey(), val);
                            }
                        }

                        scala.Option<URL> data =
                            environment.resource(ks+".json");
                        if (!data.isEmpty()) {
                            URL url = data.get();
                            Logger.debug("loading data for \""+ks+"\"..."
                                         +url);
                            try {
                                ksp.data = Json.parse(url.openStream());
                            }
                            catch (Exception ex) {
                                Logger.error("Can't load data for \""
                                             +ks+"\"!", ex);
                            }
                        }
                        
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
