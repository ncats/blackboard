package blackboard.pubmed.index;

import javax.inject.Inject;
import akka.actor.ActorSystem;
import play.libs.concurrent.CustomExecutionContext;

public class PubMedExecutionContext extends CustomExecutionContext {
    @Inject
    public PubMedExecutionContext (ActorSystem actorSystem) {
        super (actorSystem, "pubmed-dispatcher");
    }
}
