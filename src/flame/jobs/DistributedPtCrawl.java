package flame.jobs;

import flame.flame.*;
import flame.kvs.Row;
import flame.tools.Hasher;

public class DistributedPtCrawl {
    public static void run(FlameContext ctx, String[] args) throws Exception {
        // Load the data from the "pt-crawl-fake" table.
        FlameRDD rdd = ctx.fromTable("pt-crawl-fake", (Row row) -> {
            String url = row.get("url");
            String page = row.get("page");
            if (url == null || page == null) {
                return null; // skip rows without required fields
            }
            return url + "\t" + page;
        });


        FlamePairRDD pairRDD = rdd.mapToPair(line -> {
            // Parse the line
            String[] parts = line.split("\t", 2);
            if (parts.length < 2) {
                return null; // skip malformed lines
            }
            String url = parts[0];
            String page = parts[1];

            // Use the hash of the URL as the key to achieve a good distribution
            String key = Hasher.hash(url);
            return new FlamePair(key, url + "\t" + page);
        });

        // The underlying KVS should distribute these rows across the cluster by their hashed keys.

        pairRDD.saveAsTable("pt-crawl-new-fake");
        ctx.output("Data has been re-stored in a sharded manner into 'pt-crawl' table.");
    }
}
