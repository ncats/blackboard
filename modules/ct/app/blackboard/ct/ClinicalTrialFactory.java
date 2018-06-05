package blackboard.ct;
import java.io.File;

public interface ClinicalTrialFactory {
    ClinicalTrialDb get (File dbdir);
}
