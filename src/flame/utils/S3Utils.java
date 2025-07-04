package flame.utils;

import java.io.ByteArrayOutputStream;
import java.util.List;

import flame.tools.Logger;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;

import java.io.InputStream;

public class S3Utils {
    private static AmazonS3 s3Client;
    private static final Logger logger = Logger.getLogger(S3Utils.class);
    static {
        try {
            s3Client = AmazonS3ClientBuilder.standard()
                    .withRegion("us-east-1")
                    .build();
        } catch (Exception e) {
            logger.error("Error initializing S3 client", e);
        }
    }

    public static List<String> listAllKeys(String bucket, String prefix) {
        // List all objects under the prefix
        ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucket).withPrefix(prefix);
        ListObjectsV2Result result;
        java.util.ArrayList<String> keys = new java.util.ArrayList<>();
        do {
            result = s3Client.listObjectsV2(req);
            for (S3ObjectSummary summary : result.getObjectSummaries()) {
                keys.add(summary.getKey());
            }
            req.setContinuationToken(result.getNextContinuationToken());
        } while (result.isTruncated());
        return keys;
    }

    public static byte[] readObjectBytes(String bucket, String key) throws Exception {
        S3Object s3obj = s3Client.getObject(bucket, key);
        try (InputStream in = s3obj.getObjectContent(); ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[8192];
            int read;
            while ((read = in.read(buffer)) != -1) {
                bos.write(buffer, 0, read);
            }
            return bos.toByteArray();
        }
    }
}