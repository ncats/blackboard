package blackboard.firebase.test;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.firestore.FirestoreOptions;
import com.google.cloud.firestore.Firestore;
import com.google.firebase.cloud.FirestoreClient;
import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import com.google.cloud.firestore.QuerySnapshot;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.QueryDocumentSnapshot;
import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.WriteResult;
import com.google.cloud.firestore.SetOptions;
import com.google.cloud.firestore.EventListener;
import com.google.cloud.firestore.ListenerRegistration;
import com.google.cloud.firestore.FirestoreException;
import com.google.cloud.firestore.DocumentChange;
import com.google.cloud.Timestamp;

import java.io.InputStream;
import java.io.FileInputStream;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.logging.Logger;
import java.util.logging.Level;

import org.junit.rules.ExternalResource;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TestName;
import static org.junit.Assert.assertTrue;


/**
 * sbt firebase/"testOnly blackboard.firebase.test.TestFirebase"
 */
public class TestFirebase {
    static final Logger logger =
        Logger.getLogger(TestFirebase.class.getName());

    private Firestore db;
    
    @Rule public TestName name = new TestName();
    @Rule public ExternalResource resource = new ExternalResource () {
            @Override
            protected void before () throws Throwable {
                InputStream serviceAccount =
                    new FileInputStream ("serviceAccountTest.json");
                GoogleCredentials credentials =
                    GoogleCredentials.fromStream(serviceAccount);
                FirestoreOptions options = FirestoreOptions.newBuilder()
                    .setCredentials(credentials)
                    .setTimestampsInSnapshotsEnabled(true)
                    .build();
                db = options.getService();
            }
            
            @Override
            protected void after () {
                try {
                    db.close();
                }
                catch (Exception ex) {
                    logger.log(Level.SEVERE, "Can't close database!", ex);
                }
            }
        };

            
    public TestFirebase () {
    }

    @Test
    public void testFirebaseRead () throws Exception {
        // Use a service account
        for (CollectionReference colRef : db.listCollections()) {
            logger.info("Collection: "+colRef.getId()+"/");
            for (DocumentReference docRef : colRef.listDocuments()) {
                logger.info("  + Document: "
                            +docRef.getId() +" "+docRef.getPath());
            }
        }
    }

    @Test
    public void testFirebaseAdd () throws Exception {
        CollectionReference colRef = db.collection("bogus-collection");
        logger.info("bogus collection: "+colRef.getPath());
        Map<String, Object> data = new HashMap<>();
        data.put("created", Timestamp.now());
        data.put("title", "this is a bogus document");
        data.put("count", 1234554);
        data.put("value", 3.1415926);
        ApiFuture<DocumentReference> future = colRef.add(data);
        DocumentReference docRef = future.get();
        logger.info("Document "+docRef.getId()+" created!");
    }

    @Test
    public void testFirebaseDelete () throws Exception {
        CollectionReference colRef = db.collection("bogus-collection");
        int count = 0;
        for (DocumentReference docRef : colRef.listDocuments()) {
            logger.info("Deleting document "+docRef.getId());
            docRef.delete().get();
            ++count;
        }
        logger.info(count+" document(s) deleted!");
    }
}
