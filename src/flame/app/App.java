package flame.app;

import static flame.webserver.Server.*;
import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;

import flame.webserver.Server;
import flame.webserver.Server.staticFiles;
import flame.kvs.KVSClient;
import flame.search.Search;
import flame.search.Search.*;

class App {
    public static void main(String args[]) throws IOException {
        if (args.length != 1) {
            System.err.println("Syntax: app <kvsCoordinator>");
            System.exit(1);
        }

        port(8080);
        // staticFiles.location("frontend");

        // Server.get("/", (req, res) -> {
        // res.redirect("/test.html", 301);
        // return null;
        // });
        // Server.get("/results", (req, res) -> {
        // res.redirect("/search-results.html", 301);
        // return null;
        // });

        Server.get("/search", (req, res) -> {
            KVSClient kvs = new KVSClient(args[0]);
            int numResults = 10;

            if (req.queryParams("q") == null) {
                return "[]";
            }

            if (req.queryParams("n") != null) {
                try {
                    numResults = Integer.parseInt(req.queryParams("n"));
                } catch (NumberFormatException | NullPointerException e) {
                }
            }

            String searchString = URLDecoder.decode(req.queryParams("q"), StandardCharsets.UTF_8);
            List<String> searchResults = Search.simpleSearchWithTFIDF(kvs, searchString, numResults);
            if (searchResults != null) {
                // debug
                // for (String s : searchResults) {
                // System.out.println(s);
                // }
                return "[" + String.join(",", searchResults) + "]";
            }

            return "[]";
        });
        // debug dynamic route

        Server.get("/hello/:name", (req, res) -> {
            return "Hello " + req.params("name");
        });
    }
}