package flame.jobs;

import flame.flame.FlameContext;
import flame.kvs.KVSClient;
import flame.kvs.Row;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class IDF {

    private static final String CONSOLIDATED_TABLE = "pt-index";
    private static final String OUTPUT_TABLE = "pt-idf";

    public static int calculateTotalDocuments(KVSClient kvs) throws IOException {
        Iterator<Row> consolidatedEntries = kvs.scan(CONSOLIDATED_TABLE);
        Set<String> uniqueUrls = new HashSet<>();

        while (consolidatedEntries.hasNext()) {
            Row row = consolidatedEntries.next();
            String urls = row.get("urls");
            if (urls != null && !urls.isEmpty()) {
                String[] urlArray = urls.split(",");
                for (String url : urlArray) {
                    uniqueUrls.add(url.trim());
                }
            }
        }

        System.out.println("Total number of unique documents (URLs): " + uniqueUrls.size());
        return uniqueUrls.size();
    }

    public static void calculateIDF(KVSClient kvs, int totalDocuments) throws IOException {
        Iterator<Row> consolidatedEntries = kvs.scan(CONSOLIDATED_TABLE);

        while (consolidatedEntries.hasNext()) {
            Row row = consolidatedEntries.next();
            String key = row.key();
            if (key == null || key.isEmpty()) {
                System.out.println("Invalid key encountered (null or empty). Skipping...");
                continue;
            }

            String urls = row.get("urls");
            if (urls == null || urls.isEmpty()) {
                System.out.println("Key: " + key + ", URLs: empty, Outcome: skipped");
                continue;
            }

            // Calculate document frequency (number of unique URLs)
            int documentFrequency = urls.split(",").length;

            // Calculate IDF: log(totalDocuments / (1 + documentFrequency))
            double idf = Math.log((double) totalDocuments / (1 + documentFrequency));

            // Store IDF in the output table
            kvs.put(OUTPUT_TABLE, key, "idf", String.valueOf(idf));
            System.out.println("Key: " + key + ", Document Frequency: " + documentFrequency + ", IDF: " + idf);
        }
    }

    public static void run(FlameContext flameContext, String[] args) {
        try {
            KVSClient kvs = flameContext.getKVS();

            // Calculate total number of documents
            int totalDocuments = calculateTotalDocuments(kvs);

            // Calculate IDF for each key
            calculateIDF(kvs, totalDocuments);
            System.out.println("Total Documents: " + totalDocuments);
            flameContext.output("Total Documents: " + totalDocuments);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
