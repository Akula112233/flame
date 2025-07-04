package flame.jobs;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import flame.flame.FlameContext;
import flame.flame.FlamePair;
import flame.flame.FlamePairRDD;
import flame.kvs.Row;
import flame.tools.Hasher;
import flame.tools.Logger;

public class PageRank {
    private static final Logger logger = Logger.getLogger(PageRank.class);

    public static void run(FlameContext ctx, String[] args) throws Exception {
        logger.info("PageRank job started.");
        double CONVERGENCE_THRESH = Double.parseDouble(args[0]);
        double minConvergenceRatio = 1;
        if (args.length == 2) {
            minConvergenceRatio = Double.parseDouble(args[1]) / 100.0;
        }
        logger.info("Convergence threshold: " + CONVERGENCE_THRESH + ", minRatio: " + minConvergenceRatio);

        FlamePairRDD previousStateTable = ctx.fromTable("pt-crawl", row -> {
            String urlHash = row.key();
            String page = row.get("page");
            String baseUrl = row.get("url");
            if (baseUrl == null) baseUrl = "null-url";
            if (page == null) page = "";
            List<String> normalizedLinks = extractUrls(page, baseUrl);
            List<String> linkHashes = new ArrayList<>();
            for (String link : normalizedLinks) {
                linkHashes.add(Hasher.hash(link));
            }
            return urlHash + ",1.0,1.0," + String.join(",", linkHashes);
        }).mapToPair(str -> {
            int firstComma = str.indexOf(",");
            if (firstComma == -1) {
                logger.info("Warning: no comma in PageRank input: " + str);
                return new FlamePair("invalid", "");
            }
            String key = str.substring(0, firstComma);
            String val = str.substring(firstComma + 1);
            return new FlamePair(key, val);
        });

        logger.info("Initial state table for PageRank created.");

        while (true) {
            FlamePairRDD aggregatedRanks = previousStateTable.flatMapToPair(pair -> {
                List<FlamePair> rankContributions = new ArrayList<>();
                String[] data = pair._2().split(",", 3);
                double currentRank = Double.parseDouble(data[0]);
                String links = data.length > 2 ? data[2] : "";

                if (!links.isEmpty()) {
                    String[] outgoing = links.split(",");
                    double rankContribution = 0.85 * currentRank / outgoing.length;
                    for (String li : outgoing) {
                        rankContributions.add(new FlamePair(li, Double.toString(rankContribution)));
                    }
                }
                rankContributions.add(new FlamePair(pair._1(), "0.0"));
                return rankContributions;
            }).foldByKey("0.0", (acc, v) -> Double.toString(Double.parseDouble(acc) + Double.parseDouble(v)));

            FlamePairRDD newStateTable = previousStateTable.join(aggregatedRanks).flatMapToPair(joinedPair -> {
                String dataToSplit = joinedPair._2();
                int firstComma = dataToSplit.indexOf(",");
                int secondComma = dataToSplit.indexOf(",", firstComma + 1);
                int lastComma = dataToSplit.lastIndexOf(",");

                String oldCurrentRank = dataToSplit.substring(0, firstComma);
                String links = dataToSplit.substring(secondComma + 1, lastComma);
                String newRankStr = dataToSplit.substring(lastComma + 1);

                double newRank = 0.15 + Double.parseDouble(newRankStr);
                return Collections.singletonList(new FlamePair(joinedPair._1(), newRank + "," + oldCurrentRank + "," + links));
            });

            previousStateTable = newStateTable;

            List<String> differences = newStateTable.flatMap(pair -> {
                String[] ranks = pair._2().split(",");
                return Collections.singletonList(
                        Double.toString(Math.abs(Double.parseDouble(ranks[0]) - Double.parseDouble(ranks[1]))));
            }).collect();

            int numConverged = 0;
            for (String diff : differences) {
                if (Double.parseDouble(diff) <= CONVERGENCE_THRESH) {
                    numConverged++;
                }
            }

            double ratio = (double) numConverged / differences.size();
            logger.info("PageRank iteration: " + differences.size() + " nodes, " + numConverged + " converged, ratio=" + ratio);
            if (ratio >= minConvergenceRatio) {
                logger.info("Convergence reached.");
                break;
            }
        }

        previousStateTable.flatMapToPair(pair -> {
            Row row = new Row(pair._1());
            row.put("rank", pair._2().split(",", 3)[0]);
            ctx.getKVS().putRow("pt-pageranks", row);
            return Collections.emptyList();
        });
        logger.info("PageRank table pt-pageranks saved. PageRank job finished.");
    }

    private static String urlToAbsolute(String baseUrl, String candidateUrl) {
        try {
            URL absoluteUrl = new URL(new URL(baseUrl), candidateUrl);
            int port = absoluteUrl.getPort();
            if (port == -1) {
                port = absoluteUrl.getDefaultPort() != -1 ? absoluteUrl.getDefaultPort() :
                        (absoluteUrl.getProtocol().equalsIgnoreCase("http") ? 80 : 443);
                absoluteUrl = new URL(absoluteUrl.getProtocol(), absoluteUrl.getHost(), port, absoluteUrl.getFile());
            }
            if (absoluteUrl.getPath().isEmpty()) {
                absoluteUrl = new URL(absoluteUrl.getProtocol(), absoluteUrl.getHost(), absoluteUrl.getPort(), "/");
            }
            if (!"http".equalsIgnoreCase(absoluteUrl.getProtocol()) &&
                    !"https".equalsIgnoreCase(absoluteUrl.getProtocol())) {
                return null;
            }
            if (absoluteUrl.getPath().matches(".*\\.(jpg|jpeg|png|gif|svg|css|js)$")) {
                return null;
            }
            return absoluteUrl.toString();
        } catch (MalformedURLException e) {
            // Just print locally
            System.out.println("malformed url: " + candidateUrl);
            e.printStackTrace();
        }
        return null;
    }

    public static List<String> extractUrls(String pageContent, String baseUrl) {
        List<String> urls = new ArrayList<>();
        Pattern pattern = Pattern.compile("<a[^>]*href=[\"']([^\"'#>]+)[\"'][^>]*>", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(pageContent);
        while (matcher.find()) {
            String href = matcher.group(1).split("#")[0].trim();
            String absoluteUrl = urlToAbsolute(baseUrl, href);
            // Debug via System.out since ctx not available here directly
            System.out.println("extracting url: " + absoluteUrl);
            urls.add(absoluteUrl);
        }
        return urls;
    }
}
