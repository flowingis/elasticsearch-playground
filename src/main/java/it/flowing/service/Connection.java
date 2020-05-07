package it.flowing.service;

import javax.enterprise.context.ApplicationScoped;
import java.io.IOException;

@ApplicationScoped
public class Connection {

    private static final String PROTOCOL = "http";
    private static final int DEFAULT_PORT = 9200;

    public Client open(String host, int port) throws IllegalArgumentException {
        checkHost(host);
        port = checkPort(port);

        return Client.newClient(host, port);
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

    public void close(Client client) throws IOException {
        if (null != client) {
            client.close();
        }
    }

}
