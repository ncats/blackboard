package blackboard.aws.test;

import java.io.*;
import java.util.logging.Logger;
import java.util.logging.Level;

import org.junit.rules.ExternalResource;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TestName;
import static org.junit.Assert.assertTrue;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.comprehendmedical.AWSComprehendMedical;
import com.amazonaws.services.comprehendmedical.AWSComprehendMedicalClient;
import com.amazonaws.services.comprehendmedical.model.DetectEntitiesRequest;
import com.amazonaws.services.comprehendmedical.model.DetectEntitiesResult;

/*
 * sbt aws/"testOnly blackboard.aws.test.TestComprehend"
 * these tests assume that your credentials are properly setup in the
 * file ~/.aws/credentials, e.g., 
[default]
aws_access_key_id = XXXXXXX
aws_secret_access_key = YYYYYYYYYYYYYY
 */
public class TestComprehend {
    static final Logger logger =
        Logger.getLogger(TestComprehend.class.getName());

    AWSComprehendMedical comprehendmedical;
    
    @Rule public TestName name = new TestName();
    @Rule public ExternalResource resource = new ExternalResource () {
            @Override
            protected void before () throws Throwable {
                /*
                final BasicAWSCredentials awsCreds =
                    new BasicAWSCredentials("", "");
                */

                comprehendmedical =
                    AWSComprehendMedicalClient.builder().defaultClient();
                /*
                    AWSComprehendMedicalClient.builder().standard()
                    .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
                    .withRegion("us-east-1")
                    .build();*/
            }
            
            @Override
            protected void after () {
                try {
                    comprehendmedical.shutdown();
                }
                catch (Exception ex) {
                    logger.log(Level.SEVERE, "Can't shutdown AWS service!", ex);
                }
            }
        };

    public TestComprehend () {
    }

    @Test
    public void testMedicalComprehend () throws Exception {
        DetectEntitiesRequest request =
            new DetectEntitiesRequest().withText("cerealx 84 mg daily");
        DetectEntitiesResult result = comprehendmedical.detectEntities(request);
        result.getEntities().forEach(System.out::println);
    }
}
