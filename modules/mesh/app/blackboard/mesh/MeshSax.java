package blackboard.mesh;

import java.io.*;
import java.util.*;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.function.Consumer;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.*;

import javax.xml.parsers.*;
import org.xml.sax.helpers.*;
import org.xml.sax.*;

/*
 * simple utility to dump out mesh tree number to csv:
 * sbt mesh/"run-main blackboard.mesh.MeshSax desc2018.gz"
 */
public class MeshSax extends DefaultHandler {
    StringBuilder content = new StringBuilder ();
    LinkedList<String> stack = new LinkedList<>();
    String ui, name;
    List<String> treeNumbers = new ArrayList<>();
    
    public MeshSax () {
        
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
        case "DescriptorRecord":
            treeNumbers.clear();
            ui = null;
            name = null;
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
        case "DescriptorUI":
            if ("DescriptorRecord".equals(parent)) {
                ui = value;
            }
            break;
        case "String":
            if ("DescriptorName".equals(parent) && stack.size() == 3) {
                name = value;
            }
            break;
        case "TreeNumber":
            if ("TreeNumberList".equals(parent)) {
                treeNumbers.add(value);
            }
            break;
        case "DescriptorRecord":
            for (String tr : treeNumbers) {
                System.out.println(ui+",\""+name+"\","+tr);
            }
            break;
        }
    }
    
    public void characters (char[] ch, int start, int length) {
        content.append(ch, start, length);
    }
        
    public static void main (String[] argv) throws Exception {
        if (argv.length == 0) {
            System.err.println("Usage: blackboard.mesh.MeshSax desc2018.gz");
            System.exit(1);
        }
        
        MeshSax sax = new MeshSax ();
        sax.parse(new GZIPInputStream (new FileInputStream (argv[0])));
    }
}
