package flame.kvs;

import flame.tools.Logger;

import java.io.IOException;

import static flame.webserver.Server.get;
import static flame.webserver.Server.port;

public class Coordinator extends flame.generic.Coordinator {
    private static final Logger logger = Logger.getLogger(Coordinator.class);

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            String message = "Incorrect Usage: Please provide <port> as first argument";
            logger.error(message);
            System.err.println(message);
            System.exit(1);
        }

        int portNumber = Integer.parseInt(args[0]);
        port(portNumber);
        registerRoutes();

        get("/", (req, res) -> {
            StringBuilder html = new StringBuilder();
            html.append("<html>");
            html.append("<head><title>KVS Corrdinator</title></head>");
            html.append("<body>");
            html.append("<h1>KVS Coordinator</h1>");
            html.append(workerTable());
            html.append("</body>");
            html.append("</html>");
            return html.toString();
        });
    }


}
