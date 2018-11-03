package blackboard.schemaorg.test;

import java.util.List;
import java.util.logging.Logger;
import java.util.logging.Level;

import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TestName;
import static org.junit.Assert.assertTrue;

import com.google.schemaorg.core.Thing;
import com.google.schemaorg.core.MedicalCondition;

import blackboard.schemaorg.SchemaOrgFactory;

/**
 * sbt schemaorg/"testOnly blackboard.schemaorg.test.TestSerialization"
 */
public class TestSerialization {
    static final Logger logger =
        Logger.getLogger(TestSerialization.class.getName());

    @Rule public TestName name = new TestName();
    public TestSerialization () {
    }

    @Test
    public void testSerialization1 () throws Exception {
        MedicalCondition object =
            SchemaOrgFactory.createSampleMedicalCondition();
        String jsonLdStr = SchemaOrgFactory.toJsonString(object);
        logger.info("\n"+jsonLdStr);
        
        List<Thing> objects = SchemaOrgFactory.fromJsonString(jsonLdStr);
        MedicalCondition cond = (MedicalCondition)objects.get(0);
        assertTrue ("serialization failed!",
                    jsonLdStr.equals(SchemaOrgFactory.toJsonString(cond)));
    }
}
