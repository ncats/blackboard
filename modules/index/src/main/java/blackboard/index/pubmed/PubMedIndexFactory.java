package blackboard.index.pubmed;
import java.io.File;

public interface PubMedIndexFactory {
    PubMedIndex get (File dbdir);
}
