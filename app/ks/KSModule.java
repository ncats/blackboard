package ks;

import java.util.Set;
import javax.inject.Inject;
import javax.inject.Provider;
import play.inject.NamedImpl;
import scala.collection.Seq;
import play.Logger;
import play.api.Configuration;
import play.api.Environment;
import play.api.inject.Binding;
import play.api.inject.Module;
import play.libs.Scala;
import com.typesafe.config.Config;

import com.google.common.collect.ImmutableList;
import blackboard.KSource;

public class KSModule extends Module {
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
                        KSourceProvider ksp = new KSourceProvider (klass);
                        if (conf.hasPath("name"))
                            ksp.name = conf.getString("name");
                        if (conf.hasPath("version"))
                            ksp.version = conf.getString("version");
                        if (conf.hasPath("uri"))
                            ksp.uri = conf.getString("uri");
                        list.add(bind (KSource.class)
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
