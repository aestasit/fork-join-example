package com.aestas.blog.forkjoin;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.apache.lucene.analysis.LimitTokenCountAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveAction;

import static org.apache.lucene.document.Field.Index.ANALYZED;
import static org.apache.lucene.document.Field.Index.NOT_ANALYZED;

/**
 * @author <a href="mailto:luciano@aestasit.com">Luciano Fiandesio</a>
 */
public class Indexer extends RecursiveAction {
    private IndexWriter indexWriter;
    private List<File> files;
    private final static int BATCH_SIZE = 500;
    private boolean use_new_index_folder = false;

    public Indexer(IndexWriter iw, List<File> filez) {
        this.indexWriter = iw;
        this.files = filez;
    }

    public Indexer(IndexWriter iw, List<File> filez, boolean useIndexFolder) {
        this.indexWriter = iw;
        this.files = filez;
        this.use_new_index_folder = useIndexFolder;
    }

    @Override
    protected void compute() {

        if (files.size() > BATCH_SIZE) {
            List<ForkJoinTask> x1 = new ArrayList<>();
            List<List<File>> partitioned = Lists.partition(files, BATCH_SIZE);

            for (List<File> listz : partitioned) {
                x1.add(new Indexer(indexWriter, listz, use_new_index_folder));
            }
            invokeAll(x1);

        } else {
            //System.out.println("WARNING: USING SEQUENTIAL SWEEP");
            // No parallelism
            IndexWriter localIndexWriter = null;
            if (use_new_index_folder) {
                try {
                    File indexFolder = new File("d:/temp/merge-index/" + UUID.randomUUID().toString());
                    indexFolder.mkdirs();
                    Directory fsDir = FSDirectory.open(indexFolder);
                    IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_31,
                            new LimitTokenCountAnalyzer(new StandardAnalyzer(Version.LUCENE_31), 1000));
                    localIndexWriter = new IndexWriter(fsDir, conf);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            for (File f : files) {
                indexFile(f, use_new_index_folder ? localIndexWriter : indexWriter);
            }
            if (use_new_index_folder) try {
                localIndexWriter.close();
            } catch (IOException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }


    }

    private String fileToString(File file) throws IOException {
        return Files.toString(file, Charsets.UTF_8);
    }

    private void indexFile(File file, IndexWriter _indexWriter) {
        String fileName = file.getName();

        try {
            final String text = fileToString(file);
            //numChars += text.length();
            Document d = new Document();
            d.add(new Field("file", fileName,
                    Field.Store.YES, NOT_ANALYZED));
            d.add(new Field("text", text,
                    Field.Store.YES, ANALYZED));
            _indexWriter.addDocument(d);

        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

    }
}
