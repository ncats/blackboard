package blackboard.mesh;

import java.io.*;
import java.util.*;
import java.lang.reflect.Array;
import java.util.function.Consumer;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.event.*;
import org.neo4j.graphdb.index.*;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.IndexCreator;

public class MeshDb implements Mesh, AutoCloseable {
    static final Logger logger = Logger.getLogger(MeshDb.class.getName());
    static final String INDEX_LABEL = "MSH"; // MeSH source

    static final Label DESC_LABEL = Label.label(DESC);
    static final Label QUAL_LABEL = Label.label(QUAL);
    static final Label SUPP_LABEL = Label.label(SUPP);
    static final Label PA_LABEL = Label.label(PA);
    static final Label TERM_LABEL = Label.label(TERM);
    static final Label CONCEPT_LABEL = Label.label(CONCEPT);

    final GraphDatabaseService gdb;
    final File dbdir;

    public MeshDb (File dbdir) throws IOException {
        gdb = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(dbdir)
            //.setConfig(GraphDatabaseSettings.read_only, "true")
            .newGraphDatabase();

        int files = 0;        
        try (Transaction tx = gdb.beginTx();
             ResourceIterator<Node> it =
             gdb.findNodes(Label.label("InputFile"))) {
            while (it.hasNext()) {
                Node n = it.next();
                logger.info("## name="+n.getProperty("name")
                            +" count="+n.getProperty("count"));
                ++files;
            }
            tx.success();
        }
        
        if (files == 0)
            throw new RuntimeException
                ("Not a valid MeSH database: "+dbdir);

        logger.info("## "+dbdir+" mesh database initialized...");
        this.dbdir = dbdir;
    }

    Index<Node> nodeIndex () {
        return gdb.index().forNodes(INDEX_LABEL);
    }

    Entry instrument (Entry entry, Node node) {
        entry.ui = (String) node.getProperty("ui");
        entry.name = (String) node.getProperty("name");
        if (node.hasProperty("created"))
            entry.created = new Date ((Long)node.getProperty("created"));
        if (node.hasProperty("revised"))
            entry.revised = new Date ((Long)node.getProperty("revised"));
        if (node.hasProperty("established"))
            entry.established = new Date
                ((Long)node.getProperty("established"));
        return entry;
    }

    Qualifier instrument (Qualifier qual, Node node) {
        instrument ((Entry)qual, node);
        if (node.hasProperty("treeNumbers")) {
            String[] treeNumbers = (String[])node.getProperty("treeNumbers");
            for (String tr : treeNumbers)
                qual.treeNumbers.add(tr);
        }
        for (Relationship rel : node.getRelationships
                 (RelationshipType.withName("concept"))) {
            Node n = rel.getOtherNode(node);
            qual.concepts.add(instrument (new Concept (), n));
        }
        return qual;
    }

    Concept instrument (Concept concept, Node node) {
        instrument ((Entry)concept, node);
        if (node.hasProperty("casn1"))
            concept.casn1 = (String)node.getProperty("casn1");
        if (node.hasProperty("regno"))
            concept.regno = (String)node.getProperty("regno");
        if (node.hasProperty("note"))
            concept.note = (String)node.getProperty("note");
        if (node.hasProperty("relatedRegno")) {
            String[] related = (String[])node.getProperty("relatedRegno");
            for (String r : related)
                concept.relatedRegno.add(r);
        }
        
        for (Relationship rel : node.getRelationships
                 (RelationshipType.withName("concept"))) {
            Node n = rel.getOtherNode(node);
            if (n.hasLabel(CONCEPT_LABEL)) {
                concept.relations.add
                    (new Relation ((String)n.getProperty("ui"),
                                   (String)rel.getProperty("name")));
            }
        }
        
        for (Relationship rel : node.getRelationships
                 (RelationshipType.withName("term"))) {
            Node n = rel.getOtherNode(node);
            Term term = instrument (new Term (), n);
            term.preferred = rel.hasProperty("preferred");
            concept.terms.add(term);
        }
            
        return concept;
    }

    Term instrument (Term term, Node node) {
        instrument ((Entry)term, node);
        return term;
    }

    Descriptor instrument (Descriptor desc, Node node) {
        instrument ((Entry)desc, node);
        if (node.hasLabel(PA_LABEL)) {
            // all substance relationships
            for (Relationship rel : node.getRelationships
                     (RelationshipType.withName("substance"))) {
                Node n = rel.getOtherNode(node);
                desc.pharm.add(new Entry ((String)n.getProperty("ui"),
                                          (String)n.getProperty("name")));
            }
        }
        
        String[] qualifiers = (String[]) node.getProperty("qualifiers");
        if (qualifiers != null) {
            for (String qual : qualifiers) {
                try (IndexHits<Node> hits = nodeIndex().get("name", qual)) {
                    while (hits.hasNext()) {
                        Node qn = hits.next();
                        if (qn.hasLabel(QUAL_LABEL)) {
                            desc.qualifiers.add
                                (instrument (new Qualifier (), qn));
                        }
                    }
                }
            }
        }
        return desc;
    }

    SupplementalDescriptor instrument
        (SupplementalDescriptor supp, Node node) {
        instrument ((Entry)supp, node);
        return supp;
    }
    

    Entry toEntry (Node node) {
        Entry entry = null;
        if (node.hasLabel(DESC_LABEL)) {
            entry = instrument (new Descriptor (), node);
        }
        else if (node.hasLabel(SUPP_LABEL)) {
            entry = instrument (new SupplementalDescriptor (), node);
        }
        else if (node.hasLabel(QUAL_LABEL)) {
            entry = instrument (new Qualifier (), node);
        }
        else if (node.hasLabel(CONCEPT_LABEL)) {
            entry = instrument (new Concept (), node);
        }
        else if (node.hasLabel(TERM_LABEL)) {
            entry = instrument (new Term (), node);
        }
        else {
            logger.warning("Node "+node.getId()+" is not a MeSH node!");
        }
        
        return entry;
    }

    public Entry getEntry (String ui) {
        Entry entry = null;
        try (Transaction tx = gdb.beginTx();
             IndexHits<Node> hits = nodeIndex().get("ui", ui)) {
            if (hits.hasNext()) {
                entry = toEntry (hits.next());
            }
            tx.success();
        }
        return entry;
    }

    public void close () throws Exception {
        logger.info("## shutting down MeshDb instance...");
        gdb.shutdown();
    }
}
