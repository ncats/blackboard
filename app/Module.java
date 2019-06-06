import com.google.inject.AbstractModule;
import com.google.inject.assistedinject.FactoryProvider;

import java.time.Clock;

import blackboard.Blackboard;
import blackboard.JsonCodec;
import blackboard.neo4j.Neo4jBlackboard;
import blackboard.neo4j.Neo4jJsonCodec;

import blackboard.ct.ClinicalTrialFactory;
import blackboard.ct.ClinicalTrialDb;
import blackboard.mesh.MeshFactory;
import blackboard.mesh.MeshDb;
import blackboard.index.pubmed.PubMedIndexFactory;
import blackboard.index.pubmed.PubMedIndex;

/**
 * This class is a Guice module that tells Guice how to bind several
 * different types. This Guice module is created when the Play
 * application starts.
 *
 * Play will automatically use any class called `Module` that is in
 * the root package. You can create modules in other locations by
 * adding `play.modules.enabled` settings to the `application.conf`
 * configuration file.
 */
public class Module extends AbstractModule {

    @Override
    public void configure() {
        // Use the system clock as the default implementation of Clock
        bind(Clock.class).toInstance(Clock.systemDefaultZone());
        bind(Blackboard.class).to(Neo4jBlackboard.class).asEagerSingleton();
        bind(JsonCodec.class).to(Neo4jJsonCodec.class);
        bind(ClinicalTrialFactory.class).toProvider
            (FactoryProvider.newFactory
             (ClinicalTrialFactory.class, ClinicalTrialDb.class));
        bind(MeshFactory.class).toProvider
            (FactoryProvider.newFactory(MeshFactory.class, MeshDb.class));
        bind(PubMedIndexFactory.class).toProvider
            (FactoryProvider.newFactory(PubMedIndexFactory.class,
                                        PubMedIndex.class));
    }
}
