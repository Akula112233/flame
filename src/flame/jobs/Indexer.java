package flame.jobs;

import flame.external.PorterStemmer;
import flame.flame.FlameContext;
import flame.flame.FlameRDD;
import flame.kvs.KVSClient;
import flame.kvs.Row;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class Indexer {

    private static final String INDEX_TABLE = "pt-index";
    private static final String CRAWL_TABLE = "pt-crawl";
    private static final String PROGRESS_TABLE = "progress";
    private static final HashSet<String> lines = new HashSet<>();

    public static void run(FlameContext flameContext, String[] args) throws Exception {
        KVSClient kvs = flameContext.getKVS();

        String filePath = "processed-words.txt"; // Replace with your file path

        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
                lines.add(line);
            }
        } catch (IOException e) {
            System.err.println("Error reading the file: " + e.getMessage());
        }

        FlameRDD pages = flameContext.fromTable(CRAWL_TABLE, row -> {
            String url = row.get("url");
            String page = row.get("page");

            return (url != null && page != null) ? url + "," + page : null;
        });

        // Process each page and update the index
        List<String> results = pages.collect();
        for (String result : results) {
            String[] parts = result.split(",", 2);
            String url = parts[0];
            String page = parts[1].toLowerCase();

            processPage(url, page, kvs);

            // Log progress by writing the completed URL to the progress table
            kvs.put(PROGRESS_TABLE, url, "status", "completed");
        }
    }

    private static void processPage(String url, String page, KVSClient kvs) throws IOException {
        PorterStemmer stemmer = new PorterStemmer();
        String[] words = page.replaceAll("\\<.*?>", " ") // Remove HTML tags
                .replaceAll("[^a-zA-Z0-9 ]", " ") // Remove non-alphanumeric characters
                .split("\\s+");

        Map<String, Set<String>> index = new HashMap<>();
        for (String word : words) {
            if (!word.isEmpty()) {
                stemmer.add(word.toCharArray(), word.length());
                stemmer.stem();
                String stemmedWord = stemmer.toString();

                // Check if word is in pt-index
//                Row existingRow = kvs.getRow(INDEX_TABLE, stemmedWord);
//                if (existingRow != null) { // Only proceed if the word exists in pt-index
//                    index.computeIfAbsent(stemmedWord, k -> new HashSet<>()).add(url);
//                }

                // Switched to in-memory checking of our words list - faster than reading from disk each time
                if (lines.contains(stemmedWord)) { // Only proceed if the word exists in pt-index
                    index.computeIfAbsent(stemmedWord, k -> new HashSet<>()).add(url);
                }
            }
        }
        updateInvertedIndex(index, kvs);
    }

    private static void updateInvertedIndex(Map<String, Set<String>> index, KVSClient kvs) throws IOException {
        for (Map.Entry<String, Set<String>> entry : index.entrySet()) {
            String word = entry.getKey();
            Set<String> urls = entry.getValue();

            // Fetch existing row
            Row row = kvs.getRow(INDEX_TABLE, word);
            String existingUrls = (row != null) ? row.get("urls") : "";

            // Merge new URLs with existing data
            Set<String> mergedUrls = new HashSet<>(urls);
            if (!existingUrls.isEmpty()) {
                mergedUrls.addAll(Arrays.asList(existingUrls.split(",")));
            }

            // Update the KVS
            String newValue = mergedUrls.stream()
                    .sorted()
                    .collect(Collectors.joining(","));
            kvs.put(INDEX_TABLE, word, "urls", newValue);
        }
    }
}