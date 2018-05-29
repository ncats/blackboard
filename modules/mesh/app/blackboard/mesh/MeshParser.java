package blackboard.mesh;

import java.util.logging.Logger;
import java.util.logging.Level;
import java.io.*;
import java.util.zip.*;
import java.util.*;
import java.net.URI;
import java.net.URL;
import java.util.function.Consumer;
import java.security.MessageDigest;
import java.security.DigestInputStream;

import javax.xml.parsers.*;
import org.xml.sax.helpers.*;
import org.xml.sax.*;

import play.libs.Json;

/*
 * parse XML MeSH files (desc, supp, qual, and pa) available from 
 *    https://www.nlm.nih.gov/mesh/download_mesh.html
 */
public class MeshParser extends DefaultHandler {
    static final Logger logger = Logger.getLogger(MeshParser.class.getName());

    StringBuilder content = new StringBuilder ();
    Map<String, Consumer<Entry>> consumers = new HashMap<>();
    LinkedList<String> path = new LinkedList<String>();
    
    Descriptor desc;
    SupplementalDescriptor suppl;
    PharmacologicalAction pa;
    Concept concept;
    Term term;
    Qualifier qualifier;
    Entry entry;
    Relation relation;
    Calendar date = Calendar.getInstance();
    
    public MeshParser () {
        this (null);
    }
    
    public MeshParser (Consumer<Entry> consumer) {
        this (consumer, "DescriptorRecord", "SupplementalRecord",
              "QualifierRecord", "PharmacologicalAction");
    }
    
    public MeshParser (Consumer<Entry> consumer, String... names) {
        setConsumer (consumer, names);
    }

    public void setConsumer (Consumer<Entry> consumer, String... names) {
        for (String n : names)
            consumers.put(n, consumer);
    }
    public Consumer<Entry> getConsumer (String name) {
        return consumers.get(name);
    }

    public void parse (String uri) throws Exception {
        URI u = new URI (uri);
        parse (u.toURL().openStream());
    }

    public void parse (File file) throws Exception {
        if (file.isDirectory()) {
            for (File f : file.listFiles()) {
                if (f.isFile()) {
                    parseFile (f);
                }
            }
        }
        else {
            parseFile (file);
        }
    }

    public String parseFile (File file) throws Exception {
        MessageDigest md = MessageDigest.getInstance("sha");
        if (file.getName().endsWith(".zip")) {
            try (ZipInputStream zis = new ZipInputStream
                 (new DigestInputStream (new FileInputStream (file), md))) {
                ZipEntry ze = zis.getNextEntry();
                logger.info("## parsing "+ze.getName()
                            +"/"+file.getName()+"...");
                parse (zis);
            }
        }
        else if (file.getName().endsWith(".gz")) {
            try (InputStream is = new GZIPInputStream
                 (new DigestInputStream (new FileInputStream (file), md))) {
                logger.info("## parsing "+file.getName()+"...");
                parse (is);
            }
        }
        else {
            try (InputStream is = new DigestInputStream
                 (new FileInputStream (file), md)) {
                logger.info("## parsing "+file.getName()+"...");
                parse (is);
            }
        }
        
        byte[] digest = md.digest();
        StringBuilder sha = new StringBuilder ();
        for (int i = 0; i < digest.length; ++i)
            sha.append(String.format("%1$02x", digest[i] & 0xff));
        return sha.toString();
    }

    public void parse (InputStream is) throws Exception {
        SAXParser parser = SAXParserFactory.newInstance().newSAXParser();
        parser.parse(is, this);
    }

    /**
     * DefaultHandler
     */
    @Override
    public InputSource resolveEntity (String pubId, String sysId) {
        logger.warning("** Ignore external ref "
                       +(pubId != null ? pubId+":":"")+sysId);
        return new InputSource(new StringReader(""));
    }
    
    @Override
    public void characters (char[] ch, int start, int length) {
        for (int i = start, j = 0; j < length; ++j, ++i) {
            content.append(ch[i]);
        }
    }

    @Override
    public void startElement (String uri, String localName, 
                              String qName, Attributes attrs) {
        String pref;
        switch (qName) {
        case "DescriptorRecord":
            desc = new Descriptor ();
            concept = null;
            term = null;
            qualifier = null;
            entry = null;
            break;
            
        case "SupplementalRecord":
            suppl = new SupplementalDescriptor ();
            desc = null;
            concept = null;
            term = null;
            qualifier = null;
            break;

        case "QualifierRecord":
            qualifier = new Qualifier ();
            break;

        case "HeadingMappedTo":
            suppl.mapped.add(desc = new Descriptor ());
            break;

        case "IndexingInformation":
            suppl.indexed.add(desc = new Descriptor ());
            break;
            
        case "Concept":
            if (isChildOf ("QualifierRecord"))
                qualifier.concepts.add(concept = new Concept ());
            else if (isChildOf ("SupplementalRecord"))
                suppl.concepts.add(concept = new Concept ());
            else
                desc.concepts.add(concept = new Concept ());
            pref = attrs.getValue("PreferredConceptYN");
            concept.preferred = "Y".equals(pref);
            break;
            
        case "Term":
            concept.terms.add(term = new Term ());
            pref = attrs.getValue("ConceptPreferredTermYN");
            term.preferred = "Y".equals(pref);
            break;
            
        case "QualifierReferredTo":
            desc.qualifiers.add(qualifier = new Qualifier ());
            break;
            
        case "PharmacologicalAction":            
            break;

        case "PharmacologicalActionSubstanceList":
            if (entry == null)
                throw new RuntimeException
                    ("No entry for PharmalogicalAction created!");
            pa = new PharmacologicalAction (entry.ui, entry.name);
            break;

        case "Substance":
            pa.substances.add(entry = new Entry ());
            break;
            
        case "DescriptorReferredTo":
            entry = new Entry ();
            break;
            
        case "ConceptRelation":
            relation = new Relation ();
            relation.name = attrs.getValue("RelationName");
            break;
        }
        
        content.setLength(0);
        path.push(qName);        
    }

    @Override
    public void startDocument () {
        path.clear();
    }
    
    @Override
    public void endElement (String uri, String localName, String qName) {
        String value = content.toString().trim();

        path.pop();
        if (value.length() == 0)
            return;
        
        String parent = path.peek();
        Consumer<Entry> consumer = consumers.get(qName);
        
        switch (qName) {
        case "DescriptorUI":
            if (isChildOf ("HeadingMappedTo")
                || isChildOf ("IndexingInformation")) {
                desc.ui = value;
            }
            else if (parent.equals("DescriptorReferredTo")) {
                entry.ui = value;
            }
            else {
                desc.ui = value;
            }
            break;

        case "SupplementalRecordUI":
            suppl.ui = value;
            break;
            
        case "ConceptUI":
            concept.ui = value;
            break;

        case "RecordUI":
            if (parent.equals("Substance"))
                entry.ui = value;
            break;

        case "Concept1UI":
            /*
            if (!value.equals(concept.ui)) {
                // reverse direction
                concept.relations.add(relation);
            }
            */
            break;

        case "Frequency":
            if (isChildOf ("SupplementalRecord"))
                suppl.freq = Integer.parseInt(value);
            break;
            
        case "Note":
            if (isChildOf ("SupplementalRecord"))
                suppl.note = value;
            break;
            
        case "Concept2UI":
            if (!value.equals(concept.ui)) {
                relation.ui = value;
                concept.relations.add(relation);
            }
            break;

        case "RelatedRegistryNumber":
            concept.relatedRegno.add(value);
            break;
            
        case "TermUI":
            term.ui = value;
            break;
            
        case "QualifierUI":
            qualifier.ui = value;
            break;
            
        case "Abbreviation":
            qualifier.abbr = value;
            break;
            
        case "TreeNumber":
            if (isChildOf ("QualifierRecord"))
                qualifier.treeNumbers.add(value);
            else
                desc.treeNumbers.add(value);
            break;
            
        case "CASN1Name":
            concept.casn1 = value;
            break;
            
        case "RegistryNumber":
            if (!"0".equals(value))
                concept.regno = value;
            break;
            
        case "ScopeNote":
            concept.note = value;
            break;
            
        case "String":
            switch (parent) {
            case "DescriptorName":
                // see if we're in PharmacologicalAction
                if (isChildOf ("PharmacologicalAction")) {
                    entry.name = value;
                    if (isChildOf ("SupplementalRecord"))
                        suppl.pharm.add(entry);
                    else if (desc != null)
                        desc.pharm.add(entry);
                }
                else
                    desc.name = value;
                break;

            case "SupplementalRecordName":
                suppl.name = value;
                break;
                
            case "QualifierName":
                qualifier.name = value;
                break;
                
            case "ConceptName":
                concept.name = value;
                break;
                
            case "Term":
                term.name = value;
                break;

            case "RecordName":
                entry.name = value;
                break;
            }
            break;
            
        case "Year":
            date.set(Calendar.YEAR, Integer.parseInt(value));
            break;

        case "Month":
            date.set(Calendar.MONTH, Integer.parseInt(value));
            break;

        case "Day":
            date.set(Calendar.DAY_OF_MONTH, Integer.parseInt(value));
            break;

        case "DateCreated":
            if (isChildOf ("Term")) {
                term.created = date.getTime();
            }
            else if (isChildOf ("QualifierRecord")) {
                qualifier.created = date.getTime();
            }
            else if (isChildOf ("DescriptorRecord")) {
                desc.created = date.getTime();
            }
            else if (isChildOf ("SupplementalRecord")) {
                suppl.created = date.getTime();
            }
            break;

        case "DateRevised":
            if (isChildOf ("Term")) {
                term.revised = date.getTime();
            }
            else if (isChildOf ("QualifierRecord")) {
                qualifier.revised = date.getTime();
            }
            else if (isChildOf ("DescriptorRecord")) {
                desc.revised = date.getTime();
            }
            else if (isChildOf ("SupplementalRecord")) {
                suppl.revised = date.getTime();
            }
            break;

        case "DateEstablished":
            if (isChildOf ("Term")) {
                term.established = date.getTime();
            }
            else if (isChildOf ("QualifierRecord")) {
                qualifier.established = date.getTime();
            }
            else if (isChildOf ("DescriptorRecord")) {
                desc.established = date.getTime();
            }
            else if (isChildOf ("SupplementalRecord")) {
                suppl.established = date.getTime();
            }
            break;

        case "Source":
            if (isChildOf ("SupplementalRecord"))
                suppl.sources.add(value);
            break;
            
        case "DescriptorRecord":
            if (consumer != null && desc != null)
                consumer.accept(desc);
            break;

        case "SupplementalRecord":
            if (consumer != null && suppl != null)
                consumer.accept(suppl);
            break;

        case "PharmacologicalAction":
            if (consumer != null && pa != null)
                consumer.accept(pa);
            break;

        case "QualifierRecord":
            if (consumer != null && qualifier != null)
                consumer.accept(qualifier);
            break;
        }
    }

    boolean isChildOf (String tag) {
        for (String p : path)
            if (tag.equals(p))
                return true;
        return false;
    }

    public static void main (String[] argv) throws Exception {
        MeshParser parser = new MeshParser (d -> {
                /*
                  System.out.println("++ "+d.ui+" "+d.name);
                  System.out.println(d.qualifiers.size()+" qualifiers");
                  System.out.println(d.concepts.size()+" concepts");
                  System.out.println(d.pharm.size()+" pharmalogical actions");
                  System.out.println(d.treeNumbers);
                */
                System.out.println(Json.prettyPrint(Json.toJson(d)));
            });
        
        if (argv.length == 0) {
            logger.info("** reading from stdin **");
            parser.parse(System.in);            
        }
        else {
            parser.parse(new File (argv[0]));
        }
    }
}
