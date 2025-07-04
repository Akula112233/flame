package flame.jobs;

import flame.flame.FlameContext;
import flame.flame.FlameRDD;
import flame.kvs.KVSClient;
import flame.kvs.Row;
import flame.tools.Logger;

import java.util.Arrays;
import java.util.List;

public class MinimalTestJob {
    private static final Logger logger = Logger.getLogger(MinimalTestJob.class);

    public static void run(FlameContext ctx, String[] args) throws Exception {
        // Create a small in-memory list
        List<String> data = Arrays.asList("hello", "world", "test", "job");

        // Parallelize into an RDD
        FlameRDD upperRDD = ctx.parallelize(data);

        // Collect and print
        List<String> collected = upperRDD.collect();
        for (String val : collected) {
            logger.info("Val: " + val + "\n");
        }

        // Save to KVS table
        KVSClient kvs = ctx.getKVS();
        String tableName = "test_table";
        kvs.delete(tableName); // clear old data if present
        for (String val : collected) {
            Row r = new Row(val);
            r.put("original", val.toLowerCase());
            r.put("upper", val);
            kvs.putRow(tableName, r);
        }

        logger.info("MinimalTestJob completed. Table: " + tableName);
    }
}
