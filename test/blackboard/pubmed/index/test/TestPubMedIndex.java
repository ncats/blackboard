package blackboard.pubmed.index.test;

import java.util.logging.Logger;
import java.util.logging.Level;

import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import blackboard.pubmed.index.*;

public class TestPubMedIndex {
    static final Logger logger =
        Logger.getLogger(TestPubMedIndex.class.getName());

    PubMedIndex index;
    
    @Rule public TestName name = new TestName ();
    @Rule public TemporaryFolder tempdir = new TemporaryFolder();
    
    @Rule public ExternalResource resource = new ExternalResource () {
            @Override
            protected void before () throws Throwable {
                index = new PubMedIndex (tempdir.newFolder());
            }

            @Override
            protected void after () {
                try {
                    index.close();
                    tempdir.delete();
                }
                catch (Exception ex) {
                    logger.log(Level.SEVERE,
                               "Can't shutdown PubMedIndex resource!", ex);
                }
            }
        };
    
    @Test
    public void testPubMedIndex () {
        
    }
}
