package blackboard.index;

import play.Logger;
import play.libs.Json;

import javax.inject.Inject;
import java.io.*;
import java.util.*;
import java.util.zip.GZIPOutputStream;
import java.util.zip.GZIPInputStream;
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
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.search.*;
import org.apache.lucene.util.QueryBuilder;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.vectorhighlight.FastVectorHighlighter;
import org.apache.lucene.search.vectorhighlight.FieldQuery;

import com.google.inject.assistedinject.Assisted;

public class Index implements AutoCloseable {
    public static final String FIELD_TEXT = "text";
    
    public static class FV {
        public FV parent;
        public Integer total;        
        public final String label;
        public final Integer count;
        public final List<FV> children = new ArrayList<>();

        protected FV (String label, Integer count) {
            this.label = label;
            this.count = count;
        }

        public FV add (FV node) {
            node.parent = this;
            children.add(node);
            return this;
        }
        
        public String[] toPath () {
            List<String> path = new ArrayList<>();
            for (FV p = this; p != null; p = p.parent) {
                path.add(0, p.label);
            }
            return path.toArray(new String[0]);
        }

        public String toPath (String sep) {
            StringBuilder sb = new StringBuilder ();
            for (FV p = this; p != null; ) {
                FV parent = p.parent;
                sb.insert(0, parent != null ? sep+p.label : p.label);
                p = parent;
            }
            return sb.toString();
        }

        static void print (StringBuilder sb, FV fv) {
            for (FV p = fv; p != null; p = p.parent)
                sb.append(" ");
            sb.append(fv.label+" ("+fv.count+")\n");
            for (FV child : fv.children)
                print (sb, child);
        }
        
        public String toString () {
            StringBuilder sb = new StringBuilder ();
            print (sb, this);
            return sb.toString();
        }
    }
    
    public static class Facet {
        public final String name;
        public final List<FV> values = new ArrayList<>();

        protected Facet (String name) {
            this.name = name;
        }

        public String toString () {
            StringBuilder sb = new StringBuilder ();
            sb.append("["+name+"]\n");
            for (FV fv : values)
                FV.print(sb, fv);
            return sb.toString();
        }
    }

    protected class ResultDoc {
        public final Document doc;
        public final int docId;
        public final IndexReader reader;
        public final FieldQuery fq;
        public final FastVectorHighlighter fvh;

        ResultDoc (Document doc, int docId, IndexReader reader,
                   FieldQuery fq, FastVectorHighlighter fvh) {
            this.doc = doc;
            this.docId = docId;
            this.reader = reader;
            this.fq = fq;
            this.fvh = fvh;
        }

        public String[] getFragments (String field, int length, int nfrags)
            throws IOException {
            return fvh.getBestFragments
                (fq, reader, docId, field, length, nfrags);
        }
    }

    protected static abstract class SearchResult {
        public final List<Facet> facets = new ArrayList<>();
        public final int fdim;
        
        protected SearchResult () {
            this (100);
        }
        protected SearchResult (int fdim) {
            this.fdim = fdim;
        }
        protected abstract boolean process (ResultDoc doc);
    }

    final protected FieldType tvFieldType;
    final protected File root;
    final protected Directory indexDir;
    final protected IndexWriter indexWriter;
    final protected Directory taxonDir;
    final protected DirectoryTaxonomyWriter taxonWriter;
    final protected FacetsConfig facetConfig;
    final protected SearcherManager searcherManager;

    protected Index (File dir) throws IOException {
        File text = new File (dir, "text");
        text.mkdirs();
        indexDir = new NIOFSDirectory (text.toPath());
        indexWriter = new IndexWriter
            (indexDir, new IndexWriterConfig (new StandardAnalyzer ()));
        File taxon = new File (dir, "taxon");
        taxon.mkdirs();
        taxonDir = new NIOFSDirectory (taxon.toPath());
        taxonWriter = new DirectoryTaxonomyWriter (taxonDir);
        facetConfig = configFacets ();

        tvFieldType = new FieldType (TextField.TYPE_STORED);
        tvFieldType.setStoreTermVectors(true);
        tvFieldType.setStoreTermVectorPositions(true);
        tvFieldType.setStoreTermVectorPayloads(true);
        tvFieldType.setStoreTermVectorOffsets(true);
        tvFieldType.freeze();

        searcherManager = new SearcherManager
            (indexWriter, new SearcherFactory ());
        this.root = dir;
    }

    /*
     * should be overriden by subclass!
     */
    protected FacetsConfig configFacets () {
        return new FacetsConfig ();
    }

    public File getDbFile () { return root; }
    public void close () throws Exception {
        searcherManager.close();
        IOUtils.close(indexWriter, indexDir, taxonWriter, taxonDir);
    }
    public int size () {
        return indexWriter.numDocs();
    }

    public String toString () {
        return "###"+getClass().getName()+": db="+root+" size="+size();
    }
    
    public Index addIndexes (File... dbs) throws IOException {
        DirectoryTaxonomyWriter.OrdinalMap map =
            new DirectoryTaxonomyWriter.MemoryOrdinalMap();
        for (File f : dbs) {
            if (!f.exists()) {
                Logger.error(f+": not exist!");
            }
            else {
                File text = new File (f, "text");
                if (text.exists()) {
                    Directory index = new NIOFSDirectory (text.toPath());
                    Directory taxon = new NIOFSDirectory
                        (new File (f, "taxon").toPath());
                    TaxonomyMergeUtils.merge
                        (index, taxon, map, indexWriter,
                         taxonWriter, facetConfig);
                    IOUtils.close(index, taxon);
                }
                else {
                    Logger.error(f+": not a valid pubmed index!");
                }
            }
        }
        return this;
    }

    void getFacetHierarchy (FV fv, Facets facets, String dim, int topN)
        throws IOException {
        FacetResult fr = facets.getTopChildren(topN, dim, fv.toPath());
        if (fr != null) {
            for (int i = 0; i < fr.labelValues.length; ++i) {
                LabelAndValue lv = fr.labelValues[i];
                FV node = new FV (lv.label, lv.value.intValue());
                fv.add(node);
                getFacetHierarchy (node, facets, dim, topN);
            }
        }
    }

    protected List<Facet> toFacets (Facets facets, int topN)
        throws IOException {
        List<Facet> lf = new ArrayList<>();
        for (FacetResult fr : facets.getAllDims(topN)) {
            FacetsConfig.DimConfig dimconf = facetConfig.getDimConfig(fr.dim);
            Facet f = new Facet (fr.dim);
            if (dimconf.hierarchical) {
                for (int i = 0; i < fr.labelValues.length; ++i) {
                    LabelAndValue lv = fr.labelValues[i];
                    FV root = new FV (lv.label, lv.value.intValue());
                    f.values.add(root);
                    getFacetHierarchy (root, facets, fr.dim, topN);
                }
            }
            else {
                for (int i = 0; i < fr.labelValues.length; ++i) {
                    LabelAndValue lv = fr.labelValues[i];
                    FV fv = new FV (lv.label, lv.value.intValue());
                    f.values.add(fv);
                }
            }
            lf.add(f);
        }
        return lf;
    }

    protected int search (Query query, SearchResult results)
        throws Exception {
        try (IndexReader reader = DirectoryReader.open(indexWriter);
             TaxonomyReader taxonReader =
             new DirectoryTaxonomyReader (taxonWriter)) {
            
            FastVectorHighlighter fvh = new FastVectorHighlighter ();
            FieldQuery fq = fvh.getFieldQuery(query, reader);
            IndexSearcher searcher = new IndexSearcher (reader);
            FacetsCollector fc = new FacetsCollector ();
            TopDocs docs = FacetsCollector.search
                (searcher, query, reader.maxDoc(), fc);
            Facets facets = new FastTaxonomyFacetCounts
                (taxonReader, facetConfig, fc);
            int nd = 0;
            for (; nd < docs.totalHits; ++nd) {
                int docId = docs.scoreDocs[nd].doc;
                ResultDoc rdoc = new ResultDoc
                    (searcher.doc(docId), docId, reader, fq, fvh);
                if (!results.process(rdoc))
                    break;
            }
            results.facets.addAll(toFacets (facets, results.fdim));
            
            return nd;
        }
    }
    
    public static void main (String[] argv) throws Exception {
        if (argv.length < 2) {
            System.err.println("Index INDEXDB INDEXES...");
            System.exit(1);
        }
        try (Index index = new Index (new File (argv[0]))) {
            List<File> files = new ArrayList<>();
            for (int i = 1; i < argv.length; ++i)
                files.add(new File (argv[i]));
            index.addIndexes(files.toArray(new File[0]));
        }
    }
}
