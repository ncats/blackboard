package blackboard.pubmed;

import java.io.*;
import java.util.*;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.zip.GZIPInputStream;
import java.util.function.Consumer;
import java.util.concurrent.atomic.AtomicInteger;

import blackboard.mesh.Entry;
import blackboard.mesh.Descriptor;
import blackboard.mesh.Qualifier;
import blackboard.mesh.MeshDb;
import blackboard.mesh.MeshKSource;
import blackboard.mesh.Concept;
import blackboard.mesh.CommonDescriptor;
import blackboard.neo4j.Neo4j;

import com.google.inject.assistedinject.Assisted;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.*;

import javax.xml.parsers.*;
import org.xml.sax.helpers.*;
import org.xml.sax.*;

import play.Logger;
import play.libs.F;
import play.libs.ws.*;
import play.inject.ApplicationLifecycle;
import javax.inject.Inject;

public class PubMedDb extends Neo4j implements AutoCloseable {
    static final public RelationshipType MESH_RELTYPE =
        RelationshipType.withName("mesh");
        
    class PubMedSax extends DefaultHandler {
        StringBuilder content = new StringBuilder ();
        LinkedList<String> stack = new LinkedList<>();
        PubMedDoc doc;
        Calendar cal = Calendar.getInstance();
        String idtype, ui, majorTopic;
        MeshHeading mh;
        Consumer<PubMedDoc> consumer;
        
        PubMedSax (Consumer<PubMedDoc> consumer) {
            this.consumer = consumer;
        }
        
        public void parse (InputStream is) throws Exception {
            SAXParserFactory.newInstance().newSAXParser().parse(is, this);
        }
        
        public void startDocument () {
        }
        
        public void endDocument () {
        }
        
        public void startElement (String uri, String localName, String qName, 
                                  Attributes attrs) {
            switch (qName) {
            case "PubmedArticle":
                doc = new PubMedDoc ();
                break;
            case "PubDate":
                cal.clear();
                break;
            case "ArticleId":
                idtype = attrs.getValue("IdType");
                break;
                
            case "DescriptorName":
                majorTopic = attrs.getValue("MajorTopicYN");
                // fallthrough
                
            case "NameOfSubstance":
            case "QualifierName":
                ui = attrs.getValue("UI");
                break;

            case "MeshHeading":
                mh = null;
                break;
            }
            stack.push(qName);
            content.setLength(0);
        }
        
        public void endElement (String uri, String localName, String qName) {
            stack.pop();
            String parent = stack.peek();
            String value = content.toString();
            switch (qName) {
            case "PMID":
                if ("MedlineCitation".equals(parent)) {
                    try {
                        doc.pmid = Long.parseLong(value);
                    }
                    catch (NumberFormatException ex) {
                        Logger.error("Bogus PMID: "+content, ex);
                    }
                }
                break;
                
            case "Year":
                if ("PubDate".equals(parent))
                    cal.set(Calendar.YEAR, Integer.parseInt(value));
                break;
            case "Month":
                if ("PubDate".equals(parent))
                    cal.set(Calendar.MONTH, PubMedDoc.parseMonth(value));
                break;
            case "Day":
                if ("PubDate".equals(parent))
                    cal.set(Calendar.DAY_OF_MONTH, Integer.parseInt(value));
                break;
                
            case "PubDate":
                doc.date = cal.getTime();
                break;

            case "Title":
                if ("Journal".equals(parent))
                    doc.journal = value;
                break;

            case "AbstractText":
                if ("Abstract".equals(parent))
                    doc.abs.add(value);
                break;

            case "ArticleTitle":
                doc.title = value;
                break;

            case "ArticleId":
                if ("doi".equals(idtype))
                    doc.doi = value;
                else if ("pmc".equals(idtype))
                    doc.pmc = value;
                break;
                
            case "NameOfSubstance":
                if (ui != null) {
                    Entry chem = mesh.getEntry(ui);
                    if (chem != null)
                        doc.chemicals.add(chem);
                }
                break;

            case "DescriptorName":
                if ("MeshHeading".equals(parent)) {
                    Entry desc = mesh.getEntry(ui);
                    if (desc != null) {
                        mh = new MeshHeading
                            (desc, "Y".equals(majorTopic));
                        doc.headings.add(mh);
                    }
                }
                break;

            case "QualifierName":
                if (mh != null) {
                    Entry qual = mesh.getEntry(ui);
                    if (qual != null)
                        mh.qualifiers.add(qual);
                }
                break;

            case "PubmedArticle":
                if (consumer != null)
                    consumer.accept(doc);
                break;
            }
        }
        
        public void characters (char[] ch, int start, int length) {
            content.append(ch, start, length);
        }
    } // PubMedSax
    
    class PubMedNodeFactory extends UniqueFactory.UniqueNodeFactory {
        PubMedNodeFactory () {
            super (getDbIndex ());
        }

        @Override
        protected void initialize (Node node, Map<String, Object> props) {
            for (Map.Entry<String, Object> me : props.entrySet()) {
                node.setProperty(me.getKey(), me.getValue());
            }
            node.setProperty("created", System.currentTimeMillis());
            node.addLabel(Label.label("article"));
        }
    }

    class MeshNodeFactory extends UniqueFactory.UniqueNodeFactory {
        MeshNodeFactory () {
            super (getNodeIndex ("mesh"));
        }
        
        @Override
        protected void initialize (Node node, Map<String, Object> props) {
            for (Map.Entry<String, Object> me : props.entrySet()) {
                node.setProperty(me.getKey(), me.getValue());
            }
            node.setProperty("created", System.currentTimeMillis());
            node.addLabel(Label.label("mesh"));
        }
    }

    WSClient wsclient;    
    final MeshDb mesh;
    final PubMedNodeFactory pmf;
    final MeshNodeFactory mnf;

    @Inject
    public PubMedDb (WSClient wsclient, MeshKSource mesh,
                     ApplicationLifecycle lifecycle,
                     @Assisted File dbdir) {
        this (dbdir, mesh.getMeshDb());
        
        if (lifecycle != null) {
            lifecycle.addStopHook(() -> {
                    wsclient.close();
                    return F.Promise.pure(null);
                });
        }
        this.wsclient = wsclient;
    }
    
    public PubMedDb (File dbdir, MeshDb mesh) {
        super (dbdir);
        
        try (Transaction tx = gdb.beginTx()) {
            pmf = new PubMedNodeFactory ();
            mnf = new MeshNodeFactory ();
            tx.success();
        }
        this.mesh = mesh;
    }

    @Override
    public void shutdown () throws Exception {
        Logger.debug("## shutting down PubMedDb instance "+dbdir+"...");
        super.shutdown();
        
        if (wsclient != null)
            wsclient.close();
    }
    
    public void close () throws Exception {
        try {
            shutdown ();
        }
        catch (Exception ex) {
            Logger.error("Can't shutdown database", ex);
        }
    }

    UniqueFactory.UniqueEntity<Node> add (PubMedDoc d) {
        UniqueFactory.UniqueEntity<Node> uf = pmf
            .getOrCreateWithOutcome("pmid", d.pmid);
        if (!uf.wasCreated())
            return uf;
        
        Node node = uf.entity();
        node.setProperty("title", d.title);
        if (!d.abs.isEmpty())
            node.setProperty("abstract", d.abs.toArray(new String[0]));
        node.setProperty("journal", d.journal);
        node.setProperty("date", d.date.getTime());
        Calendar cal = Calendar.getInstance();
        cal.setTime(d.date);
        node.setProperty("year", cal.get(Calendar.YEAR));
        
        addTextIndex (node, d.title);
        for (String text : d.abs)
            addTextIndex (node, text);
        for (Entry e : d.chemicals)
            add (e, node, null);
        for (MeshHeading mh : d.headings)
            add (mh, node);
            
        return uf;
    }

    UniqueFactory.UniqueEntity<Node> add
        (Entry e, Node node, Boolean major, Entry... quals) {
         UniqueFactory.UniqueEntity<Node> uf =
            mnf.getOrCreateWithOutcome("ui", e.ui);
         
         Node n = uf.entity();        
        if (uf.wasCreated()) {
            // initialize...
            n.setProperty("name", e.name);
            addTextIndex (node, e.name);
            
            CommonDescriptor cd = (CommonDescriptor)e;
            for (Concept c : cd.getConcepts()) {
                if (c.regno != null)
                    addTextIndex (node, c.regno);
                for (String r : c.relatedRegno)
                    addTextIndex (node, r);
                addTextIndex (node, c.name);
            }

            List<String> pharm = new ArrayList<>();
            for (Entry a : cd.getPharmacologicalActions()) {
                //n.addLabel(Label.label(a.name));
                addTextIndex (node, a.name);
                pharm.add(a.name);
            }
            
            if (!pharm.isEmpty())
                n.setProperty("pharmacological", pharm.toArray(new String[0]));

            if (e instanceof Descriptor) {
                Descriptor desc = (Descriptor)e;
                List<String> paths = new ArrayList<>();
                for (String tr : desc.treeNumbers) {
                    String[] toks = tr.split("\\.");
                    StringBuilder path = new StringBuilder (toks[0]);
                    n.addLabel(Label.label(path.toString()));
                    for (int i = 1; i < toks.length; ++i) {
                        //n.addLabel(Label.label(path.toString()));
                        paths.add(path.toString());
                        path.append("."+toks[i]);
                        addTextIndex (node, path.toString());
                    }
                    paths.add(path.toString());
                    addTextIndex (node, path.toString());
                }
                
                if (!desc.treeNumbers.isEmpty()) {
                    n.setProperty("treeNumbers", desc.treeNumbers.toArray
                                  (new String[0]));
                }
            }
        }

        RelationshipType type = RelationshipType.withName
            (e.getClass().getSimpleName());
        for (Relationship rel : node.getRelationships(type)) {
            if (rel.getOtherNode(node).equals(n))
                return uf;
        }
        
        Relationship rel = node.createRelationshipTo(n, type);
        List<String> qualifiers = new ArrayList<>();
        for (Entry q : quals)
            qualifiers.add(((Qualifier)q).name);
        
        if (major != null)
            rel.setProperty("majorTopic", major);
        
        if (!qualifiers.isEmpty()) {
            rel.setProperty("qualifiers",
                            qualifiers.toArray(new String[0]));
        }
        
        return uf;
    }

    UniqueFactory.UniqueEntity<Node> add (MeshHeading mh, Node node) {
        return add (mh.descriptor, node, mh.majorTopic,
                    mh.qualifiers.toArray(new Entry[0]));
    }

    public int add (InputStream is) throws Exception {
        AtomicInteger count = new AtomicInteger ();
        SimpleDateFormat sdf = new SimpleDateFormat ("yyyy.MM.dd");
        new PubMedSax(d -> {
                try (Transaction tx = gdb.beginTx()) {
                    add (d);
                    tx.success();
                }
                Logger.debug(count.incrementAndGet()+" "+d.pmid+" "
                             +sdf.format(d.date)+" chem="+d.chemicals.size()
                             +" mh="+d.headings.size());
                /*
                for (MeshHeading mh : d.headings) {
                    Logger.debug(mh.descriptor.ui+"...quals="
                                 +mh.qualifiers.size());
                }
                */
        }).parse(is);

        return count.get();
    }

    /*
     * sbt pubmed/"runMain blackboard.pubmed.PubMedDb ..."
     */
    public static void main (String[] argv) throws Exception {
        if (argv.length < 3) {
            System.err.println
                ("Usage: blackboard.pubmed.PubMedDb DBDIR MESHDB FILES...");
            System.exit(1);
        }

        try (MeshDb mesh = new MeshDb (null, new File (argv[1]));
             PubMedDb pdb = new PubMedDb (new File (argv[0]), mesh)) {
            for (int i = 2; i < argv.length; ++i) {
                pdb.add(new GZIPInputStream (new FileInputStream (argv[i])));
            }
        }
    }
}

