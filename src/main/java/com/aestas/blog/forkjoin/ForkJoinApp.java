package com.aestas.blog.forkjoin;

import com.google.common.collect.Lists;
import org.apache.lucene.analysis.LimitTokenCountAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.Version;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ForkJoinPool;

/**
 * @author <a href="mailto:luciano@aestasit.com">Luciano Fiandesio</a>
 */
public class ForkJoinApp {
    private final ForkJoinPool forkJoinPool = new ForkJoinPool(2);

    private final static boolean MULTI_INDEX = true;

    public static void main(String[] args) throws IOException {
        ForkJoinApp fja = new ForkJoinApp();
        fja.startIndexing(args);
    }

    public void startIndexing(String[] args) throws IOException {

        File docDir = new File(args[0]);
        File indexDir = new File(args[1]);
        File INDEXES_DIR = new File("D:/temp/merge-index");
        deleteDir(indexDir);
        indexDir.mkdir();

        System.out.println("===== FOLDER DELETED OK...");

        long now = System.currentTimeMillis();
        Directory fsDir = FSDirectory.open(indexDir);
        IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_31,
                new LimitTokenCountAnalyzer(new StandardAnalyzer(Version.LUCENE_31), 1000));

        IndexWriter indexWriter = new IndexWriter(fsDir, conf);

        forkJoinPool.invoke(new Indexer(indexWriter, Lists.<File>newArrayList(docDir.listFiles()), MULTI_INDEX));

        if (MULTI_INDEX) {

            System.out.println("Parallel indexing (without index merge) took: " + (System.currentTimeMillis() - now));
            indexWriter.setMergeFactor(1000);
            indexWriter.setRAMBufferSizeMB(50);
            Directory indexes[] = new Directory[INDEXES_DIR.list().length];
            for (int i = 0; i < INDEXES_DIR.list().length; i++) {
                System.out.println("Adding: " + INDEXES_DIR.list()[i]);
                indexes[i] = new SimpleFSDirectory(new File(INDEXES_DIR.getAbsolutePath()
                        + "/" + INDEXES_DIR.list()[i]));
            }
            indexWriter.addIndexes(indexes);
        }
        indexWriter.optimize();
        indexWriter.close();

        System.out.println("Parallel indexing took: " + (System.currentTimeMillis() - now));

        int numDocs = indexWriter.numDocs();
        System.out.println("Number of docs: " + numDocs);

        new LuceneSearcher().search(indexDir, "+background", 1000);
    }

    public static boolean deleteDir(File dir) {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            for (int i = 0; i < children.length; i++) {
                boolean success = deleteDir(new File(dir, children[i]));
                if (!success) {
                    return false;
                }
            }
        }

        // The directory is now empty so delete it
        return dir.delete();
    }

}
