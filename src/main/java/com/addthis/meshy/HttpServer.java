package com.addthis.meshy;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.prometheus.client.exporter.MetricsServlet;

public class HttpServer {
    private static final Logger log = LoggerFactory.getLogger(HttpServer.class);

    private Server server;

    public HttpServer(int port) {
        this.server = new Server(port);
        ServletContextHandler context = new ServletContextHandler();
        context.setContextPath("/");
        context.addServlet(new ServletHolder(new MetricsServlet()), "/metrics");
        server.setHandler(context);
    }

    public void start() throws Exception {
        log.info("Starting metrics server");
        server.start();
    }

    public void stop() {
        log.info("Stopping metrics server");
        try {
            server.stop();
        } catch (Exception e) {
            log.error("Error shutting down metrics server", e);
        }
    }
}