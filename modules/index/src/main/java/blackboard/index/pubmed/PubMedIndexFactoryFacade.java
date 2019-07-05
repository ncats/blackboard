package blackboard.index.pubmed;
import java.io.File;

public interface PubMedIndexFactoryFacade {
    PubMedIndex get (File dbdir);
}
