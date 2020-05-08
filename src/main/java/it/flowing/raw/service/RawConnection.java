package it.flowing.raw.service;

import javax.enterprise.context.ApplicationScoped;
import java.io.IOException;

@ApplicationScoped
public class RawConnection {

    private static final String PROTOCOL = "http";
    private static final int DEFAULT_PORT = 9200;

    public RawClient open(String host, int port) throws IllegalArgumentException {
        checkHost(host);
        port = checkPort(port);

        return RawClient.newClient(host, port);
    }

    private int checkPort(int port) {
        if (0 == port) {
            port = DEFAULT_PORT;
        }
        return port;
    }

    private void checkHost(String host) {
        if (null == host || host.isEmpty()) {
            throw new IllegalArgumentException();
        }
    }

    public void close(RawClient client) throws IOException {
        if (null != client) {
            client.close();
        }
    }

}
