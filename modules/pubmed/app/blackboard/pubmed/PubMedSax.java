package blackboard.pubmed;

import blackboard.mesh.Entry;
import blackboard.mesh.Descriptor;
import blackboard.mesh.Qualifier;
import blackboard.mesh.MeshDb;
import blackboard.mesh.MeshKSource;
import blackboard.mesh.Concept;
import blackboard.mesh.CommonDescriptor;

import javax.xml.parsers.*;
import org.xml.sax.helpers.*;
import org.xml.sax.*;

import java.util.*;
import java.io.InputStream;
import java.util.function.Consumer;

import play.Logger;

public class PubMedSax extends DefaultHandler {
    StringBuilder content = new StringBuilder ();
    LinkedList<String> stack = new LinkedList<>();
    PubMedDoc doc;
    Calendar cal = Calendar.getInstance();
    String idtype, ui, majorTopic;
    MeshHeading mh;
    final MeshDb mesh;
    Consumer<PubMedDoc> consumer;
    
    public PubMedSax (Consumer<PubMedDoc> consumer) {
        this (null, consumer);
    }

    public PubMedSax (MeshDb mesh, Consumer<PubMedDoc> consumer) {
        this.mesh = mesh;
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
            if (ui != null && mesh != null) {
                Entry chem = mesh.getEntry(ui);
                if (chem != null)
                    doc.chemicals.add(chem);
            }
            break;
            
        case "DescriptorName":
            if (mesh != null && "MeshHeading".equals(parent)) {
                Entry desc = mesh.getEntry(ui);
                if (desc != null) {
                    mh = new MeshHeading
                        (desc, "Y".equals(majorTopic));
                    doc.headings.add(mh);
                }
            }
            break;
            
        case "QualifierName":
            if (mh != null && mesh != null) {
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

    public static void main (String[] argv) throws Exception {
        if (argv.length == 0) {
            System.err.println("Usage: blackboard.pubmed.PubMedSax FILES...");
            System.exit(1);
        }

        PubMedSax pms = new PubMedSax (d -> {
                System.out.println(d.getPMID()+": "+d.getTitle());
            });
        for (String a : argv) {
            pms.parse(new java.util.zip.GZIPInputStream
                      (new java.io.FileInputStream (a)));
        }
    }
} // PubMedSax
