import com.google.inject.AbstractModule;
import com.google.inject.assistedinject.FactoryProvider;
import com.google.inject.TypeLiteral;
import com.google.inject.PrivateModule;

import java.time.Clock;

import blackboard.Blackboard;
import blackboard.JsonCodec;
import blackboard.neo4j.Neo4jBlackboard;
import blackboard.neo4j.Neo4jJsonCodec;

import blackboard.ct.ClinicalTrialFactory;
import blackboard.ct.ClinicalTrialDb;
import blackboard.mesh.MeshFactory;
import blackboard.mesh.MeshDb;

import blackboard.pubmed.index.PubMedIndex;
import blackboard.pubmed.index.PubMed;
import blackboard.idg.index.IDGIndex;
import blackboard.idg.index.IDG;

import blackboard.index.IndexFacade;
import blackboard.index.IndexFactory;
import blackboard.index.DefaultIndexFactory;

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

        install (new PrivateModule () {
                @Override
                protected void configure () {
                    // bind IndexFacade PRIVATELY
                    bind(IndexFacade.class).toProvider
                        (FactoryProvider.newFactory(IndexFacade.class,
                                                    PubMedIndex.class));
                    bind(IndexFactory.class).annotatedWith(PubMed.class)
                        .to(DefaultIndexFactory.class);
                    // expose IndexFactory GLOBALLY
                    expose(IndexFactory.class).annotatedWith(PubMed.class);
                }
            });
        install (new PrivateModule () {
                @Override
                protected void configure () {
                    // bind IndexFacade PRIVATELY
                    bind(IndexFacade.class).toProvider
                        (FactoryProvider.newFactory(IndexFacade.class,
                                                    IDGIndex.class));
                    bind(IndexFactory.class).annotatedWith(IDG.class)
                        .to(DefaultIndexFactory.class);
                    // expose IndexFactory GLOBALLY
                    expose(IndexFactory.class).annotatedWith(IDG.class);
                }
            });        
    }
}
