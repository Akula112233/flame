package flame.jobs;

import flame.flame.FlameContext;
import flame.kvs.KVSClient;
import flame.kvs.Row;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

public class ConsolidateIndex {

    private static final String SOURCE_TABLE = "pt-index-partition-8";
    private static final String DEST_TABLE = "pt-index-partition-5";

    public static void consolidate(KVSClient kvs) throws IOException {
        Iterator<Row> sourceEntries = kvs.scan(SOURCE_TABLE);

        while (sourceEntries.hasNext()) {
            Row sourceRow = sourceEntries.next();
            String key = sourceRow.key();

            if (key == null || key.isEmpty()) {
                System.out.println("Invalid key encountered (null or empty). Skipping...");
                continue;
            }

            try {
                // Ensure the key can be safely encoded
                String encodedKey = URLEncoder.encode(key, StandardCharsets.UTF_8.toString());

                String urlsFromSource = sourceRow.get("urls");
                System.out.println(DEST_TABLE + " " + encodedKey + " " + urlsFromSource);
                Row destinationRow = kvs.getRow(DEST_TABLE, encodedKey);
                String urlsFromDestination = destinationRow != null ? destinationRow.get("urls") : null;

                if (urlsFromSource == null || urlsFromSource.isEmpty()) {
                    System.out.println("Key: " + key + ", Source URLs: empty, Destination URLs: " + (urlsFromDestination == null ? "empty" : urlsFromDestination) + ", Outcome: skipped");
                    continue;
                }

                if (destinationRow != null) {
                    String updatedUrls;
                    if (urlsFromDestination == null || urlsFromDestination.isEmpty()) {
                        updatedUrls = urlsFromSource;
                    } else {
                        updatedUrls = urlsFromDestination + "," + urlsFromSource;
                    }
                    kvs.put(DEST_TABLE, encodedKey, "urls", updatedUrls);
                    System.out.println("Key: " + key + ", Source URLs: " + urlsFromSource + ", Destination URLs: " + urlsFromDestination + ", Outcome: combined into: " + updatedUrls);
                } else {
                    kvs.put(DEST_TABLE, encodedKey, "urls", urlsFromSource);
                    System.out.println("Key: " + key + ", Source URLs: " + urlsFromSource + ", Destination URLs: empty, Outcome: added to destination");
                }
            } catch (Exception e) {
                System.out.println("Failed to process key: " + key + ". Skipping... Error: " + e.getMessage());
            }
        }
    }

    public static void run(FlameContext flameContext, String[] args) {
        try {
            KVSClient kvs = flameContext.getKVS();
            consolidate(kvs);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
