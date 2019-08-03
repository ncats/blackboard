package blackboard.disease;

import javax.inject.Inject;
import akka.actor.ActorSystem;
import play.libs.concurrent.CustomExecutionContext;

public class DiseaseExecutionContext extends CustomExecutionContext {
    @Inject
    public DiseaseExecutionContext (ActorSystem actorSystem) {
        super (actorSystem, "disease-dispatcher");
    }
}
