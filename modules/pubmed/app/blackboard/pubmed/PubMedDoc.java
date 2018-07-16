package blackboard.pubmed;

import java.util.List;
import java.util.ArrayList;
import java.util.Date;
import java.util.Calendar;
import static java.util.Calendar.*;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import blackboard.mesh.MeshDb;
import blackboard.mesh.Descriptor;
import blackboard.mesh.Qualifier;
import blackboard.mesh.Entry;

public class PubMedDoc {
    Document doc;
    Long pmid;
    String doi;
    String title;
    String abs;
    String journal;
    Date date;
    List<MeshHeading> headings = new ArrayList<>();
    List<Entry> chemicals = new ArrayList<>();
    
    protected PubMedDoc (Document doc, MeshDb mesh) {
        NodeList nodes = doc.getElementsByTagName("PMID");
        pmid = nodes.getLength() > 0
            ? Long.parseLong(((Element)nodes.item(0)).getTextContent()) : null;
        nodes = doc.getElementsByTagName("ArticleTitle");
        title = nodes.getLength() > 0
            ? ((Element)nodes.item(0)).getTextContent() : null;
        nodes = doc.getElementsByTagName("Abstract");
        abs = nodes.getLength() > 0
            ? ((Element)nodes.item(0)).getTextContent() : null;
        nodes = doc.getElementsByTagName("Journal");
        
        if (nodes.getLength() > 0) {
            Element elm = (Element)nodes.item(0);
            nodes = elm.getElementsByTagName("Title");
            journal = nodes.getLength() > 0
                ? ((Element)nodes.item(0)).getTextContent() : null;
            nodes = elm.getElementsByTagName("PubDate");
            if (nodes.getLength() > 0) {
                elm = (Element)nodes.item(0);
                
                Calendar cal = Calendar.getInstance();
                nodes = elm.getElementsByTagName("Year");
                if (nodes.getLength() > 0) {
                    cal.set(YEAR, Integer.parseInt
                            (((Element)nodes.item(0)).getTextContent()));
                }
                
                nodes = elm.getElementsByTagName("Month");
                int month = 0;
                if (nodes.getLength() > 0) {
                    String mon = ((Element)nodes.item(0)).getTextContent();
                    switch (mon) {
                    case "jan": case "Jan":
                        month = JANUARY;
                        break;
                    case "feb": case "Feb":
                        month = FEBRUARY;
                        break;
                    case "mar": case "Mar":
                        month = MARCH;
                        break;
                    case "apr": case "Apr":
                        month = APRIL;
                        break;
                    case "may": case "May":
                        month = MAY;
                        break;
                    case "jun": case "Jun":
                        month = JUNE;
                        break;
                    case "jul": case "Jul":
                        month = JULY;
                        break;
                    case "aug": case "Aug":
                        month = AUGUST;
                        break;
                    case "sep": case "Sep":
                        month = SEPTEMBER;
                        break;
                    case "oct": case "Oct":
                        month = OCTOBER;
                        break;
                    case "nov": case "Nov":
                        month = NOVEMBER;
                        break;
                    case "dec": case "Dec":
                        month = DECEMBER;
                        break;
                    default:
                        throw new RuntimeException ("Unknown month: "+mon);
                    }
                }
                cal.set(MONTH, month);
                
                nodes = elm.getElementsByTagName("Day");
                if (nodes.getLength() > 0) {
                    cal.set(DAY_OF_MONTH, Integer.parseInt
                            (((Element)nodes.item(0)).getTextContent()));
                }
                else {
                    cal.set(DAY_OF_MONTH, 1);
                }
                date = cal.getTime();
            }
        }

        nodes = doc.getElementsByTagName("ArticleId");
        for (int i = 0; i < nodes.getLength(); ++i) {
            Element elm = (Element)nodes.item(i);
            String type = elm.getAttribute("IdType");
            if ("doi".equals(type)) {
                doi = elm.getTextContent();
            }
        }

        nodes = doc.getElementsByTagName("MeshHeading");
        for (int i = 0; i < nodes.getLength(); ++i) {
            Element elm = (Element)nodes.item(i);
            NodeList nl = elm.getElementsByTagName("DescriptorName");
            MeshHeading mh = null;
            if (nl.getLength() > 0) {
                String ui = ((Element)nl.item(0)).getAttribute("UI");
                String major =
                    ((Element)nl.item(0)).getAttribute("MajorTopicYN");
                Entry desc = mesh.getEntry(ui);
                if (desc != null) {
                    mh = new MeshHeading (desc, "Y".equals(major));
                    headings.add(mh);
                }
            }

            if (mh != null) {
                nl = elm.getElementsByTagName("QualifierName");
                for (int j = 0; j < nl.getLength(); ++j) {
                    elm = (Element)nl.item(j);
                    String ui = elm.getAttribute("UI");
                    Entry qual = mesh.getEntry(ui);
                    if (qual != null)
                        mh.qualifiers.add(qual);
                }
            }
        }

        nodes = doc.getElementsByTagName("NameOfSubstance");
        for (int i = 0; i < nodes.getLength(); ++i) {
            Element elm = (Element)nodes.item(i);
            String ui = elm.getAttribute("UI");
            Entry chem = mesh.getEntry(ui);
            if (chem != null)
                chemicals.add(chem);
        }

        this.doc = doc;        
    }

    public Long getPMID () { return pmid; }
    public String getTitle () { return title; }
    public String getAbstract () { return abs; }
    public Date getDate () { return date; }
    public String getDOI () { return doi; }
    public String getJournal () { return journal; }
    public List<MeshHeading> getMeshHeadings () { return headings; }
    public List<Entry> getChemicals () { return chemicals; }
    @JsonIgnore
    public Document getDoc () { return doc; }
}
