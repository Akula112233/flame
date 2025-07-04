package flame.search;

import flame.external.PorterStemmer;
import flame.kvs.KVS;
import flame.kvs.KVSClient;
import flame.kvs.Row;
import flame.tools.Hasher;
import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Search {
    // simple search
    // assumes existence of pt-index, pt-pageranks, pt-idf, pt-tf tables
    // generate these with indexer, pagerank, and TFIDF if missing
    // input is string of words (sent from frontend), and integer n (number of
    // search results)
    // output is list of links (sent to frontend) of length <= n
    //
    public static List<String> simpleSearchWithTFIDF(KVS kvs, String searchString, int numResults) throws Exception {
        // normalize and split the search string into words
        PorterStemmer stemmer = new PorterStemmer();

        String[] searchWords = searchString.toLowerCase()
                .replaceAll("[.,:;!?’\"()\\-]", " ")
                .replaceAll("\\s+", " ")
                .trim()
                .split(" ");

        Set<String> wordSet = new HashSet<>(Arrays.asList(searchWords));
        Map<String, Double> finalScores = new HashMap<>();
        double alpha = 0.95; // weight for combining TF/IDF and pagerank

        // Scan the "pt-index" table to find matching words and their associated URLs

        for (String word : wordSet) {
            // grab all urls in the index for this word
            stemmer.add(word.toCharArray(), word.length());
            stemmer.stem();
            String stemmedWord = stemmer.toString();
            System.out.println(stemmedWord);
            byte[] urlsBytes = kvs.get("pt-index", stemmedWord, "urls");
            if (urlsBytes == null) {
                continue;
            }
            String[] urls = new String(urlsBytes, StandardCharsets.UTF_8).split(",");
            // Retrieve the IDF for the word
            double idf = 0.0;
            if (kvs.existsRow("pt-idf", stemmedWord)) {
                Row r = kvs.getRow("pt-idf", stemmedWord);
                byte[] idfBytes = kvs.get("pt-idf", stemmedWord, r.columns().iterator().next());
                if (idfBytes != null) {
                    idf = Double.parseDouble(new String(idfBytes, StandardCharsets.UTF_8));
                }
            }

            // process each URL
            for (String encodedUrl : urls) {
                String url = URLDecoder.decode(encodedUrl, StandardCharsets.UTF_8);
                String urlHash = Hasher.hash(url);

                // pagerank from urlhash
                double pageRank = 0.0;
                if (kvs.existsRow("pt-pageranks", urlHash)) {
                    byte[] rankBytes = kvs.get("pt-pageranks", urlHash, "rank");
                    if (rankBytes != null) {
                        pageRank = Double.parseDouble(new String(rankBytes, StandardCharsets.UTF_8));
                    }
                }

                // tf from word and urlhash
                double tf = 0.0;
                String tfRowKey = urlHash + "-" + stemmedWord;
                if (kvs.existsRow("pt-tf", tfRowKey)) {
                    byte[] tfBytes = kvs.get("pt-tf", tfRowKey,
                            kvs.getRow("pt-tf", tfRowKey).columns().iterator().next());
                    if (tfBytes != null) {
                        tf = Double.parseDouble(new String(tfBytes, StandardCharsets.UTF_8));
                    }
                }


                // compute score
                double score = alpha * tf * idf + (1 - alpha) * pageRank;
                // acculate scores for URLs across words
                finalScores.merge(url, score, Double::sum);
            }
        }

        // List<String> results = finalScores.entrySet().stream()
        // .sorted((e1, e2) -> Double.compare(e2.getValue(), e1.getValue())) // sort by
        // descending score
        // .limit(numResults)
        // .map(entry -> entry.getKey() + " FinalScore: " + entry.getValue())
        // .toList();
        List<String> results = finalScores.entrySet().stream()
                .sorted((e1, e2) -> Double.compare(e2.getValue(), e1.getValue())) // Sort by descending score
                .limit(numResults)
                .map(entry -> {
                    String url = entry.getKey();
                    String urlHash = Hasher.hash(url);
                    String snippet = "";

                    try {
                        // Retrieve snippet from the "pt-crawl" table
                        if (kvs.existsRow("pt-crawl", urlHash)) {
                            byte[] contentBytes = kvs.get("pt-crawl", urlHash, "page");
                            if (contentBytes != null) {
                                String content = new String(contentBytes, StandardCharsets.UTF_8);
                                // Remove HTML tags, sanitize content, and limit to 50 characters
                                content = content.replaceAll("<[^>]*>", " ")
                                        .replaceAll("\\s+", " ")
                                        .trim();
                                snippet = content.length() > 100 ? content.substring(0, 100) : content;
                            }
                        }
                    } catch (IOException ex) {
                    }

                    // Sanitize snippet for JSON
                    snippet = snippet.replace("\"", "\\\"");
                    return "{\"url\": \"" + url + "\", \"preview\": \"" + snippet + "\"}";
                })
                .toList();
        return results;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Syntax: search <kvsCoordinator>");
            System.exit(1);
        }
        System.out.println("starting...");

        KVSClient kvs = new KVSClient(args[0]);

        String searchString = "What's the mandate?";
        long startTime = System.currentTimeMillis();
        List<String> searchResults = simpleSearchWithTFIDF(kvs, searchString, 100);
        long endTime = System.currentTimeMillis();
        System.out.println("TFIDF/PageRank Search Results:");
        if (searchResults != null) {
            for (String s : searchResults) {
                System.out.println(s);
            }
        }
        System.out.println("Time taken to search: " + (endTime - startTime) + " millseconds");

    }
    // TODO, replace pagerank helper functions

}
