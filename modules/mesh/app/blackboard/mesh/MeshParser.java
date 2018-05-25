package blackboard.mesh;

import java.util.logging.Logger;
import java.util.logging.Level;
import java.io.*;
import java.util.*;
import java.net.URI;
import java.net.URL;
import java.util.function.Consumer;

import javax.xml.parsers.*;
import org.xml.sax.helpers.*;
import org.xml.sax.*;

import play.libs.Json;

public class MeshParser extends DefaultHandler {
    static final Logger logger = Logger.getLogger(MeshParser.class.getName());

    static class MeshEntry implements Comparable<MeshEntry> {
        public String ui;
        public String name;
        public Date created;
        public Date revised;
        public Date established;
        public boolean preferred;
        
        MeshEntry () {}
        MeshEntry (String ui) {
            this (ui, null);
        }
        MeshEntry (String ui, String name) {
            this.ui = ui;
            this.name = name;
        }

        public boolean equals (Object obj) {
            if (obj instanceof MeshEntry) {
                MeshEntry me =(MeshEntry)obj;
                return ui.equals(me.ui) && name.equals(me.name);
            }
            return false;
        }

        public int compareTo (MeshEntry me) {
            int d = ui.compareTo(me.ui);
            if (d == 0)
                d = name.compareTo(me.name);
            return d;
        }
    }

    public static class Term extends MeshEntry {
        Term () {}
        Term (String ui) {
            this (ui, null);
        }
        Term (String ui, String name) {
            super (ui, name);
        }
    }

    public static class Qualifier extends MeshEntry {
        public String abbr;        
        Qualifier () {}
        Qualifier (String ui) {
            this (ui, null);
        }
        Qualifier (String ui, String name) {
            super (ui, name);
        }
    }

    public static class Relation extends MeshEntry {
        Relation () {}
        Relation (String ui, String name) {
            super (ui, name);
        }
    }

    public static class Concept extends MeshEntry {
        public String casn1;
        public String regno;
        public String note;
        public List<Term> terms = new ArrayList<>();
        public List<Relation> relations = new ArrayList<>();
        public List<String> relatedRegno = new ArrayList<>();
        Concept () {}
        Concept (String ui) {
            this (ui, null);
        }
        Concept (String ui, String name) {
            super (ui, name);
        }
    }
        
    public static class Descriptor extends MeshEntry {
        public String annotation;
        public List<Qualifier> qualifiers = new ArrayList<>();
        public List<Concept> concepts = new ArrayList<>();
        public List<MeshEntry> pharm = new ArrayList<>();
        public List<String> treeNumbers = new ArrayList<>();
        
        Descriptor () {
        }
        Descriptor (String ui) {
            this (ui, null);
        }
        Descriptor (String ui, String name) {
            super (ui, name);
        }
    }

    public static class SupplementDescriptor extends MeshEntry {
        public Integer freq;
        public List<Descriptor> mapped = new ArrayList<>();
        public List<Descriptor> indexed = new ArrayList<>();
        public List<MeshEntry> pharm = new ArrayList<>();
        public List<String> sources = new ArrayList<>();
        SupplementDescriptor () {}
        SupplementDescriptor (String ui) {
            this (ui, null);
        }
        SupplementDescriptor (String ui, String name) {
            super (ui, name);
        }
    }
    
    StringBuilder content = new StringBuilder ();
    Consumer<MeshEntry> consumer;
    LinkedList<String> path = new LinkedList<String>();
    
    Descriptor desc;
    SupplementDescriptor suppl;
    Concept concept;
    Term term;
    Qualifier qualifier;
    MeshEntry entry;
    Relation relation;
    Calendar date = Calendar.getInstance();
    
    public MeshParser () {
        this (null);
    }
    public MeshParser (Consumer<MeshEntry> consumer) {
        this.consumer = consumer;
    }

    public void setConsumer (Consumer<MeshEntry> consumer) {
        this.consumer = consumer;
    }
    public Consumer<MeshEntry> getConsumer () { return consumer; }

    public void parse (String uri) throws Exception {
        parse (uri, null);
    }
    
    public void parse (String uri, Consumer<MeshEntry> consumer)
        throws Exception {
        URI u = new URI (uri);
        parse (u.toURL().openStream(), consumer);
    }

    public void parse (File file) throws Exception {
        parse (file, null);
    }
    
    public void parse (File file, Consumer<MeshEntry> consumer)
        throws Exception {
        parse (new FileInputStream (file), consumer);
    }

    public void parse (InputStream is) throws Exception {
        parse (is, null);
    }
    
    public void parse (InputStream is, Consumer<MeshEntry> consumer)
        throws Exception {
        SAXParser parser = SAXParserFactory.newInstance().newSAXParser();
        if (consumer != null)
            this.consumer = consumer;
        parser.parse(is, this);
    }

    /**
     * DefaultHandler
     */
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
            suppl = new SupplementDescriptor ();
            desc = null;
            concept = null;
            term = null;
            qualifier = null;
            break;

        case "HeadingMappedTo":
            suppl.mapped.add(desc = new Descriptor ());
            break;
            
        case "Concept":
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
            
        case "DescriptorReferredTo":
            entry = new MeshEntry ();
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
        switch (qName) {
        case "DescriptorUI":
            if (isChildOf ("HeadingMappedTo")) {
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
                    else
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
            for (String t : path) {
                switch (t) {
                case "Term":
                    term.created = date.getTime();
                    break;
                case "DescriptorRecord":
                    desc.created = date.getTime();
                    break;
                case "SupplementalRecord":
                    suppl.created = date.getTime();
                    break;
                }
            }
            break;

        case "DateRevised":
            for (String t : path) {
                switch (t) {
                case "Term":
                    term.revised = date.getTime();
                    break;
                case "DescriptorRecord":
                    desc.revised = date.getTime();
                    break;
                case "SupplementalRecord":
                    suppl.revised = date.getTime();
                    break;
                }
            }            
            break;

        case "DateEstablished":
            for (String t : path) {
                switch (t) {
                case "Term":
                    term.established = date.getTime();
                    break;
                case "DescriptorRecord":
                    desc.established = date.getTime();
                    break;
                case "SupplementalRecord":
                    suppl.established = date.getTime();
                    break;
                }
            }
            break;

        case "Source":
            if (isChildOf ("SupplementalRecord"))
                suppl.sources.add(value);
            break;
            
        case "DescriptorRecord":
            if (consumer != null)
                consumer.accept(desc);
            break;

        case "SupplementalRecord":
            if (consumer != null)
                consumer.accept(suppl);
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
            File file = new File (argv[0]);
            if (file.isDirectory()) {
            }
            else {
                parser.parse(file);
            }
        }
    }
}
