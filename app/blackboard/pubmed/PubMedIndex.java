package blackboard.pubmed;

import play.Logger;
import blackboard.pubmed.*;
import blackboard.umls.MetaMap;
import blackboard.mesh.MeshDb;
import blackboard.mesh.Descriptor;

import java.io.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.time.temporal.ChronoField;

import org.apache.lucene.store.*;
import org.apache.lucene.analysis.*;
import org.apache.lucene.index.*;
import org.apache.lucene.document.*;
import org.apache.lucene.facet.*;
import org.apache.lucene.facet.taxonomy.directory.*;
import org.apache.lucene.facet.taxonomy.*;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.search.*;

import gov.nih.nlm.nls.metamap.AcronymsAbbrevs;
import gov.nih.nlm.nls.metamap.ConceptPair;
import gov.nih.nlm.nls.metamap.Ev;
import gov.nih.nlm.nls.metamap.MatchMap;
import gov.nih.nlm.nls.metamap.Mapping;
import gov.nih.nlm.nls.metamap.MetaMapApi;
import gov.nih.nlm.nls.metamap.MetaMapApiImpl;
import gov.nih.nlm.nls.metamap.Negation;
import gov.nih.nlm.nls.metamap.PCM;
import gov.nih.nlm.nls.metamap.Phrase;
import gov.nih.nlm.nls.metamap.Position;
import gov.nih.nlm.nls.metamap.Result;
import gov.nih.nlm.nls.metamap.Utterance;

public class PubMedIndex implements AutoCloseable {
    final File root;
    final Directory indexDir;
    final IndexWriter indexWriter;
    final Directory taxonDir;
    final DirectoryTaxonomyWriter taxonWriter;
    final FacetsConfig facetConfig;

    final MetaMap metamap = new MetaMap ();
    
    public PubMedIndex (File dir) throws IOException {
        File text = new File (dir, "text");
        text.mkdirs();
        indexDir = new NIOFSDirectory (text.toPath());
        indexWriter = new IndexWriter
            (indexDir, new IndexWriterConfig (new StandardAnalyzer ()));
        File taxon = new File (dir, "taxon");
        taxon.mkdirs();
        taxonDir = new NIOFSDirectory (taxon.toPath());
        taxonWriter = new DirectoryTaxonomyWriter (taxonDir);
        facetConfig = new FacetsConfig ();
        facetConfig.setMultiValued("tr", true);
        facetConfig.setHierarchical("tr", true);
        facetConfig.setMultiValued("mesh", true);
        facetConfig.setMultiValued("semtype", true); // umls semantic types
        facetConfig.setMultiValued("source", true); // umls sources
        facetConfig.setMultiValued("concept", true);
        
        this.root = dir;
    }

    public File getDbFile () { return root; }
    public void close () throws Exception {
        IOUtils.close(indexWriter, indexDir, taxonWriter, taxonDir);
    }

    protected void metamap (Document doc, String text) {
        try {
            for (Result r : metamap.annotate(text)) {
                for (AcronymsAbbrevs abrv : r.getAcronymsAbbrevsList()) {
                    for (String cui : abrv.getCUIList())
                        doc.add(new StringField ("cui", cui, Field.Store.YES));
                }
                
                for (Utterance utter : r.getUtteranceList()) {
                    for (PCM pcm : utter.getPCMList())
                        for (Mapping map : pcm.getMappingList())
                            for (Ev ev : map.getEvList()) {
                                doc.add(new StringField
                                        ("cui", ev.getConceptId(),
                                         Field.Store.YES));
                                doc.add(new FacetField
                                        ("concept", ev.getConceptId()));
                                for (String t : ev.getSemanticTypes())
                                    doc.add(new FacetField ("semtype", t));
                                for (String s : ev.getSources())
                                    doc.add(new FacetField ("source", s));
                            }
                }
            }
        }
        catch (Exception ex) {
            Logger.error("Can't annotate doc "
                         +doc.get("pmid")+" with MetaMap", ex);
        }
    }

    public void add (PubMedDoc d) throws IOException {
        Logger.debug(d.getPMID()+": "+d.getTitle());
        Document doc = new Document ();
        doc.add(new LongField ("pmid", d.getPMID(), Field.Store.YES));
        doc.add(new TextField ("text", d.getTitle(), Field.Store.NO));
        metamap (doc, d.getTitle());
        
        for (String abs : d.getAbstract()) {
            doc.add(new TextField ("text", abs, Field.Store.NO));
            metamap (doc, abs);
        }
        doc.add(new LongField
                ("year", d.getDate().getYear(), Field.Store.YES));
        for (MeshHeading mh : d.getMeshHeadings()) {
            Descriptor desc = (Descriptor)mh.descriptor;
            doc.add(new StringField ("ui", desc.ui, Field.Store.YES));
            for (String tr : desc.treeNumbers) {
                Logger.debug("..."+tr);
                doc.add(new FacetField ("tr", tr.split("\\.")));
            }
            doc.add(new FacetField ("mesh", desc.name));
        }

        indexWriter.addDocument(facetConfig.build(taxonWriter, doc));
    }

    public int size () {
        return indexWriter.numDocs();
    }

    protected void debug () throws IOException {
        IndexSearcher searcher = null;        
        try {
            searcher = new IndexSearcher
                (DirectoryReader.open(indexWriter, true));
            TaxonomyReader taxonReader =
                new DirectoryTaxonomyReader (taxonWriter);
            FacetsCollector fc = new FacetsCollector ();
            searcher.search(new MatchAllDocsQuery (), fc);
            Facets facets = new FastTaxonomyFacetCounts
                (taxonReader, facetConfig, fc);
            /*
            Logger.debug(facets.getTopChildren
                         (20, "tr", "D02.455.426.559.847.638".split("\\."))
                         .toString());
            */
            Logger.debug(facets.getTopChildren(20, "mesh").toString());
            Logger.debug(facets.getTopChildren(20, "concept").toString());
            Logger.debug(facets.getTopChildren(20, "semtype").toString());
            Logger.debug(facets.getTopChildren(20, "source").toString());
            IOUtils.close(taxonReader);
        }
        finally {
            if (searcher != null)
                IOUtils.close(searcher.getIndexReader());
        }
    }
    
    public static void main (String[] argv) throws Exception {
        if (argv.length < 3) {
            System.err.println("Usage: PubMedIndex INDEXDB MESHDB FILES...");
            System.exit(1);
        }

        Logger.debug("## IndexDb: "+argv[0]);
        Logger.debug("##  MeshDb: "+argv[1]);

        try (MeshDb mesh = new MeshDb (new File (argv[1]));
             PubMedIndex index = new PubMedIndex (new File (argv[0]))) {

            AtomicInteger count = new AtomicInteger ();
            PubMedSax pms = new PubMedSax (mesh, d -> {
                    try {
                        index.add(d);
                        Logger.debug(d.getPMID()+": "+d.getTitle());
                        if (count.incrementAndGet() > 100) {
                            throw new RuntimeException ("done!");
                        }
                    }
                    catch (IOException ex) {
                        Logger.error("Can't index document "+d.getPMID(), ex);
                    }
                });
            
            for (int i = 2; i < argv.length; ++i) {
                Logger.debug("indexing "+argv[i]+"...");
                try {
                    pms.parse(new java.util.zip.GZIPInputStream
                              (new FileInputStream (argv[i])));
                }
                catch (RuntimeException ex) {
                    
                }
                index.debug();
            }
            Logger.debug("## index "+argv[0]+": "+index.size()+" document(s)!");
        }
    }

    public static class Search {
        public static void main (String[] argv) throws Exception {
            if (argv.length < 2) {
                System.err.println("PubMedIndex$Search INDEXDB TERMS...");
                System.exit(1);
            }

            try (PubMedIndex index = new PubMedIndex (new File (argv[0]))) {
                index.debug();
            }
        }
    }
}
