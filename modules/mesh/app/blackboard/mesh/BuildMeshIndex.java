package blackboard.mesh;

import java.io.*;
import java.util.*;
import java.util.logging.Logger;
import java.util.logging.Level;

import org.apache.lucene.store.*;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.search.*;
import org.apache.lucene.util.IOUtils;


/*
 * sbt mesh/"run-main blackboard.mesh.BuildMeshIndex OUTDIR INDIR"
 */
public class BuildMeshIndex implements AutoCloseable {
    static final Logger logger =
        Logger.getLogger(BuildMeshIndex.class.getName());

    File indir;
    File outdir;
    Directory indexDir;
    IndexWriter writer;
    IndexSearcher searcher;

    public BuildMeshIndex (File outdir, File indir) throws IOException {
        this.indir = indir;
        this.outdir = outdir;

        outdir.mkdirs();
        indexDir = new NIOFSDirectory (outdir.toPath());
        IndexWriterConfig config =
            new IndexWriterConfig (new StandardAnalyzer ());
        writer = new IndexWriter (indexDir, config);
        logger.info("Initializing "+outdir+"...");
    }

    public void build () throws Exception {
        
    }

    public void close () throws Exception {
        IOUtils.close(writer);
        IOUtils.close(indexDir);
    }
    
    public static void main (String[] argv) throws Exception {
        if (argv.length < 2) {
            System.out.println("Usage: "+BuildMeshIndex.class.getName()
                               +" OUTDIR INDIR");
            System.out.println("where OUTDIR is the output index directory "
                               +"and INDIR is a directory contains XML files");
            System.out.println
                ("downloaded from "
                 +"https://www.nlm.nih.gov/mesh/download_mesh.html");
            System.exit(1);
        }

        File indir = new File (argv[1]);
        if (!indir.exists()) {
            logger.log(Level.SEVERE,
                       "Input directory \""+argv[1]+"\" doesn't exist!");
            System.exit(1);
        }
        
        File outdir = new File (argv[0]);
        try (BuildMeshIndex mesh = new BuildMeshIndex (outdir, indir)) {
            mesh.build();
        }
    }
}
