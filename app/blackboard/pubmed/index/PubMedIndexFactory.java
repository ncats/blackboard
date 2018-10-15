package blackboard.pubmed.index;
import java.io.File;

public interface PubMedIndexFactory {
    PubMedIndex get (File dbdir);
}
