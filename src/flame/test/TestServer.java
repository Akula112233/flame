package flame.test;

import static flame.webserver.Server.*;

public class TestServer {
    public static void main(String[] args) throws Exception {
        securePort(443);
        get("/", (req, res) -> { return "Hello World - this is Group MLKJ"; });
    }
}