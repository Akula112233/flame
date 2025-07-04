package flame.jobs;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import flame.external.PorterStemmer;
import flame.flame.FlameContext;
import flame.flame.FlamePair;
import flame.flame.FlamePairRDD;
import flame.flame.FlameRDD;
import flame.tools.Hasher;

public class TFIDF {
    public static long debugDocsCovered = 0;

    public static void generateTF(FlameContext ctx, int chunk_idx) throws Exception {
        // document hash -> content
        System.out.println("starting tf");
        FlameRDD ftTmp = ctx.fromTable("pt-crawl-test-" + chunk_idx, row -> {
            return row.get("url") + "," + row.get("page");
        });
        FlamePairRDD urlPagePairs = ftTmp.mapToPair(str -> {
            String[] parts = str.split(",", 2);
            return new FlamePair(parts[0], parts[1]);
        });
        System.out.println("done url page mapping, starting word url pairs");

        // tf for each doc, combined processing to minimize allocations
        FlamePairRDD wordUrlPairs = urlPagePairs.flatMapToPair(pair -> {
            String url = URLDecoder.decode(pair._1(), StandardCharsets.UTF_8);
            String urlHash = Hasher.hash(url);
            String[] words = pair._2().toLowerCase().replaceAll("\\<.*?>", " ") // Remove HTML tags
                    .replaceAll("[^a-zA-Z0-9 ]", " ") // Remove non-alphanumeric characters
                    .split("\\s+");
            // String[] words = page

            // tokenize and calculate TF in a single pass
            Map<String, Double> tfWordToCount = new HashMap<>();
            // String[] words = pageContent.split(" ");
            PorterStemmer stemmer = new PorterStemmer();
            for (String word : words) {
                if (!word.isEmpty()) {
                    stemmer.add(word.toCharArray(), word.length());
                    stemmer.stem();
                    String stemmedWord = stemmer.toString();
                    tfWordToCount.put(stemmedWord, tfWordToCount.getOrDefault(stemmedWord, 0.0) + 1);
                }
            }

            // normalize
            tfWordToCount.replaceAll((_, count) -> count / words.length);
            List<FlamePair> pairs = new ArrayList<>();
            for (Map.Entry<String, Double> entry : tfWordToCount.entrySet()) {
                pairs.add(new FlamePair(urlHash + "-" + entry.getKey(), String.valueOf(entry.getValue())));
                // pairs.add(new FlamePair(urlHash + "-" + entry.getKey(),
                // String.valueOf(entry.getValue() / totalWords)));
            }
            debugDocsCovered++;

            if (debugDocsCovered % 500 == 0) {
                System.out.println("Documents covered: " + debugDocsCovered);
            }

            return pairs;
        });
        System.out.println("done word/url pairs, starting term frequency folding");

        // aggregate tf for each word across all documents (fold by word)
        FlamePairRDD termFrequencies = wordUrlPairs.foldByKey("", (existingTf, newTf) -> {
            if (existingTf.isEmpty()) {
                return newTf;
            }
            return String.valueOf(Double.parseDouble(existingTf) + Double.parseDouble(newTf));
        });
        System.out.println("saving");

        ftTmp.destroy();
        urlPagePairs.destroy();
        wordUrlPairs.destroy();

        // Save the result
        termFrequencies.saveAsTable("pt-tf-chunked-" + chunk_idx);
    }

    public static void generateIDF(FlameContext ctx) throws Exception {
        System.out.println("starting IDF");
        FlameRDD documents = ctx.fromTable("pt-crawl", row -> {
            return row.key() + "\t" + row.get("page");
        });

        // flatten into (word, docId) pairs
        FlameRDD wordDocPairs = documents.flatMap(row -> {
            String[] parts = row.split("\t", 2);
            String docId = parts[0];
            String content = parts[1];

            // String[] words = content.toLowerCase()
            // .replaceAll("[.,:;!?’\"()\\-]", " ")
            // .replaceAll("\\s+", " ")
            // .trim()
            // .split(" ");
            String[] words = content.toLowerCase().replaceAll("\\<.*?>", " ") // Remove HTML tags
                    .replaceAll("[^a-zA-Z0-9 ]", " ") // Remove non-alphanumeric characters
                    .split("\\s+");

            Set<String> pairs = new HashSet<>();
            PorterStemmer stemmer = new PorterStemmer();
            for (String word : words) {
                stemmer.add(word.toCharArray(), word.length());
                stemmer.stem();
                String stemmedWord = stemmer.toString();
                pairs.add(stemmedWord + "\t" + docId);
            }

            return new ArrayList<>(pairs);
        });

        System.out.println("done flattening");

        // (word, docId) pairs to (word, "1") for counting
        FlamePairRDD wordOccurrences = wordDocPairs.mapToPair(pair -> {
            String word = pair.split("\t", 2)[0];
            return new FlamePair(word, "1");
        });

        System.out.println("done folding");
        // now fold to count
        FlamePairRDD wordDocCounts = wordOccurrences.foldByKey("0", (c1, c2) -> {
            return String.valueOf(Integer.parseInt(c1) + Integer.parseInt(c2));
        });

        long totalDocuments = ctx.fromTable("pt-crawl", row -> row.key()).count();
        FlameRDD idfValues = wordDocCounts.flatMap(pair -> {
            int docCount = Integer.parseInt(pair._2());
            return Arrays.asList("" + Math.log((double) totalDocuments / docCount));
        });
        System.out.println("done, saving...");
        idfValues.saveAsTable("pt-idf");
    }

    public static void run(FlameContext ctx, String[] args) throws Exception {
        System.out.println("starting");
        // for memory performance reasons we chunk into 128 chunks.
        // these chunks can be merged afterword into a single directory
        for (int i = 1; i < 129; i++) {
            System.out.println("chunk: " + i);
            generateTF(ctx, i);
        }
        generateIDF(ctx);
    }
}