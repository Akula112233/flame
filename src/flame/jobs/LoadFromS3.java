package flame.jobs;

import flame.flame.FlameContext;
import flame.flame.FlamePair;
import flame.flame.FlamePairRDD;
import flame.flame.FlameRDD;
import flame.kvs.Row;

import flame.tools.Hasher;
import flame.tools.Logger;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

// Script to load in crawled data from S3 (for backup reasons) to pt-crawl

public class LoadFromS3 {
    private static final Logger logger = Logger.getLogger(LoadFromS3.class);

    private static List<String> listAllKeys(String bucket, String prefix) {
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        List<String> keys = new ArrayList<>();
        ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucket).withPrefix(prefix);
        ListObjectsV2Result result;
        do {
            result = s3Client.listObjectsV2(req);
            for (S3ObjectSummary summary : result.getObjectSummaries()) {
                keys.add(summary.getKey());
            }
            req.setContinuationToken(result.getNextContinuationToken());
        } while (result.isTruncated());
        return keys;
    }

    private static byte[] readObjectBytes(AmazonS3 s3, String bucket, String key) throws Exception {
        S3Object s3obj = s3.getObject(bucket, key);
        try (InputStream in = s3obj.getObjectContent();
             ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[8192];
            int read;
            while ((read = in.read(buffer)) != -1) {
                bos.write(buffer, 0, read);
            }
            return bos.toByteArray();
        }
    }

    private static String getOriginalUrl(AmazonS3 s3, String bucket, String key) {
        ObjectMetadata meta = s3.getObjectMetadata(bucket, key);
        Map<String, String> metaMap = meta.getUserMetadata();
        return metaMap.get("original-url");
    }

    public static void run(FlameContext ctx, String[] args) throws Exception {
        logger.info("LoadFromS3 job started");

        if (args.length < 2) {
            logger.info("Usage: LoadFromS3 <bucket> <prefix>");
            return;
        }

        String bucket = args[0];
        String prefix = args[1];
        logger.info("Bucket: " + bucket + ", Prefix: " + prefix);

        // 1. List all S3 keys
        List<String> allKeys = listAllKeys(bucket, prefix);
        logger.info("Number of keys found: " + allKeys.size());

        // 2. Parallelize keys
        FlameRDD keysRDD = ctx.parallelize(allKeys);
        logger.info("keysRDD created.");

        // 3. Process keys
        FlameRDD raw = keysRDD.mapPartitions(iter -> {
            // This runs on workers. We can't directly use logger.info here (it may not appear until after job completes),
            // but we trust these lines will be processed.
            AmazonS3 s3 = AmazonS3ClientBuilder.standard().build();
            List<String> results = new ArrayList<>();
            int count = 0;
            while (iter.hasNext()) {
                String key = iter.next();
                byte[] content = readObjectBytes(s3, bucket, key);
                String pageContent = new String(content, StandardCharsets.UTF_8);
                String url = getOriginalUrl(s3, bucket, key);
                if (url == null) {
                    url = "null-url";
                }
                String encoded = url + "\t" + pageContent;
                results.add(encoded);
                count++;
            }
            return results.iterator();
        });
        logger.info("raw RDD created from mapPartitions.");

        FlamePairRDD urlPageRDD = raw.mapToPair(line -> {
            int tabIndex = line.indexOf('\t');
            if (tabIndex == -1) {
                logger.info("Warning: No tab found in line: " + line.substring(0, Math.min(line.length(), 50)));
                return new FlamePair("invalidHash", "INVALID\t");
            }
            String url = line.substring(0, tabIndex);
            String page = line.substring(tabIndex + 1);
            String urlHash = Hasher.hash(url);
            return new FlamePair(urlHash, url + "\t" + page);
        });
        logger.info("urlPageRDD created from raw RDD.");

        // Write to pt-crawl
        urlPageRDD.flatMap(pair -> {
            String urlHash = pair._1();
            String[] parts = pair._2().split("\t", 2);
            String url = parts[0];
            String page = parts.length > 1 ? parts[1] : "";
            Row r = new Row(urlHash);
            r.put("url", url);
            r.put("page", page);
            r.put("length", String.valueOf(page.length()));
            ctx.getKVS().putRow("pt-crawl", r);
            return new ArrayList<>();
        });
        logger.info("pt-crawl table populated. LoadFromS3 job finished.");
    }
}
