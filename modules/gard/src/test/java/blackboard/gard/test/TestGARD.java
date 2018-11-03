package blackboard.gard.test;

import java.util.List;
import java.util.logging.Logger;
import java.util.logging.Level;

import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TestName;
import static org.junit.Assert.assertTrue;

/**
 * sbt gard/"testOnly blackboard.gard.test.TestGARD"
 */
public class TestGARD {
    static final Logger logger = Logger.getLogger(TestGARD.class.getName());

    @Rule public TestName name = new TestName();
    public TestGARD () {
    }

    @Test
    public void testSerialization1 () throws Exception {
    }
}
