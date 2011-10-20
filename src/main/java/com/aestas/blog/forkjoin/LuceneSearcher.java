package com.aestas.blog.forkjoin;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import java.io.File;

/**
 * @author <a href="mailto:luciano@aestasit.com">Luciano Fiandesio</a>
 */
public class LuceneSearcher {

    public void search(File indexDir, String query, int maxHits) {
        try {
            Directory dir = FSDirectory.open(indexDir);
            IndexReader reader = IndexReader.open(dir);
            IndexSearcher searcher = new IndexSearcher(reader);
            Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_31);
            QueryParser parser = new QueryParser(Version.LUCENE_31, "text", analyzer);

            Query q = parser.parse(query);
            TopDocs hits = searcher.search(q, maxHits);
            ScoreDoc[] scoreDocs = hits.scoreDocs;
            for (int n = 0; n < scoreDocs.length; ++n) {
                ScoreDoc sd = scoreDocs[n];
                float score = sd.score;
                int docId = sd.doc;
                Document d = searcher.doc(docId);
                String fileName = d.get("file");
                System.out.println(fileName);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
