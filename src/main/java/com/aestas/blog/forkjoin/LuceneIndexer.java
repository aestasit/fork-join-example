package com.aestas.blog.forkjoin;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.lucene.analysis.LimitTokenCountAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import java.io.File;
import java.io.IOException;

import static org.apache.lucene.document.Field.Index.ANALYZED;
import static org.apache.lucene.document.Field.Index.NOT_ANALYZED;

/**
 * @author <a href="mailto:luciano@aestasit.com">Luciano Fiandesio</a>
 */
public class LuceneIndexer {

    public static void main(String[] args) {


        try {
            new LuceneIndexer().index(args);
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (ParseException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    // Deletes all files and subdirectories under dir.
// Returns true if all deletions were successful.
// If a deletion fails, the method stops attempting to delete and returns false.
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

    public void index(String[] args) throws IOException, ParseException {


        File docDir = new File(args[0]);
        File indexDir = new File(args[1]);

        deleteDir(indexDir);
        new File("d:/temp/index").mkdir();

        long now = System.currentTimeMillis();

        Directory fsDir = FSDirectory.open(indexDir);
        IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_31,
                new LimitTokenCountAnalyzer(new StandardAnalyzer(Version.LUCENE_31), 1000));

        IndexWriter indexWriter = new IndexWriter(fsDir, conf);

        // Actual indexing
//        long numChars = 0L;
        for (File f : docDir.listFiles()) {
            indexFile(f, indexWriter);

        }
        indexWriter.optimize();
        indexWriter.close();
        System.out.println("Indexing time : " + (System.currentTimeMillis() - now));
        int numDocs = indexWriter.numDocs();
        System.out.println("Number of docs: " + numDocs);


        new LuceneSearcher().search(indexDir, "+background", 1000);


    }

    private String fileToString(File file) throws IOException {
        return Files.toString(file, Charsets.UTF_8);

    }

    private void indexFile(File file, IndexWriter indexWriter) {
        String fileName = file.getName();
        try {
            final String text = fileToString(file);
            //numChars += text.length();
            Document d = new Document();
            d.add(new Field("file", fileName,
                    Field.Store.YES, NOT_ANALYZED));
            d.add(new Field("text", text,
                    Field.Store.YES, ANALYZED));
            indexWriter.addDocument(d);
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

    }
}
