package com.globo.grou.undertowloader.services;

import io.undertow.UndertowOptions;
import io.undertow.client.*;
import io.undertow.connector.ByteBufferPool;
import io.undertow.protocols.ssl.UndertowXnioSsl;
import io.undertow.server.DefaultByteBufferPool;
import io.undertow.util.AttachmentKey;
import io.undertow.util.Headers;
import io.undertow.util.Methods;
import io.undertow.util.StringReadChannelListener;
import org.apache.http.conn.ssl.SSLContextBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.xnio.*;
import org.xnio.channels.StreamSinkChannel;
import org.xnio.ssl.XnioSsl;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

@Service
public class LoaderService {

    private static final AttachmentKey<String> RESPONSE_BODY = AttachmentKey.create(String.class);
    private static final ByteBufferPool pool = new DefaultByteBufferPool(true, 8192 * 3, 1000, 10, 100);
    private static final OptionMap.Builder builder = OptionMap.builder()
                                                        .set(Options.WORKER_IO_THREADS, 8)
                                                        .set(Options.TCP_NODELAY, true)
                                                        .set(Options.KEEP_ALIVE, true)
                                                        .set(Options.WORKER_NAME, "Client");

    private static final Logger LOGGER = LoggerFactory.getLogger(LoaderService.class);
    private static final long REQUEST_TIMEOUT = 1000L;
    private static final int MIN_TOTAL_DURATION_SEC = 60;
    private static final int NUM_USERS = 100;
    private static final int NUM_PARALLEL_REQUESTS = 100;
    private static final java.net.URI URI;
    private final AtomicLong requestsTotal = new AtomicLong(0L);

    static {
        String uriTemp = System.getenv("TARGET");
        if (uriTemp == null) {
            URI = java.net.URI.create("https://127.0.0.1:8443/");
        } else {
            URI = java.net.URI.create(!uriTemp.endsWith("/") ? uriTemp + "/" : uriTemp);
        }
    }

    public void run() {
        LOGGER.warn("Running loader");
        long startLoad = System.currentTimeMillis();
        long start = System.currentTimeMillis();
        IntStream.rangeClosed(0, NUM_USERS).parallel().forEach(u -> doUserRequests(startLoad));
        long duration = System.currentTimeMillis() - start;
        LOGGER.info("Duration (ms): " + duration);
        LOGGER.info("Requests total: " + requestsTotal.get());
        LOGGER.info("Num users: " + NUM_USERS);

        LOGGER.warn("Finished loader");
    }

    private void doUserRequests(long startLoad) {
        final Xnio xnio = Xnio.getInstance();
        final UndertowClient client = UndertowClient.getInstance();
        final SSLContext clientSslContext;
        final XnioSsl ssl;
        try {
            clientSslContext = new SSLContextBuilder().loadTrustMaterial(null, (certificate, authType) -> true).build();
            ssl = new UndertowXnioSsl(Xnio.getInstance(), OptionMap.EMPTY, clientSslContext);
        } catch (NoSuchAlgorithmException | KeyManagementException | KeyStoreException e) {
            LOGGER.error(e.getMessage(), e);
            return;
        }

        // create connection
        final XnioWorker xnioWorker;
        final ClientConnection connection;
        try {
            xnioWorker = xnio.createWorker(null, builder.getMap());
            connection = client.connect(URI, xnioWorker, ssl, pool, OptionMap.create(UndertowOptions.ENABLE_HTTP2, true)).get();
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
            return;
        }

        // do requests
        while (System.currentTimeMillis() - startLoad < (MIN_TOTAL_DURATION_SEC * 1000)) {
            final var numRequestsRef = new AtomicInteger(NUM_PARALLEL_REQUESTS);
            IntStream.rangeClosed(1, NUM_PARALLEL_REQUESTS).parallel().forEach(i -> sendRequest(connection, numRequestsRef, startLoad));
            long start = System.currentTimeMillis();
            while (numRequestsRef.get() > 0 && System.currentTimeMillis() - start < REQUEST_TIMEOUT) {
                try {
                    TimeUnit.MICROSECONDS.sleep(10);
                } catch (InterruptedException e) {
                    LOGGER.error(e.getMessage(), e);
                }
            }
        }

        // close connection
        try {
            connection.close();
        } catch (Exception e) {
            if (LOGGER.isDebugEnabled()) LOGGER.error(e.getMessage(), e);
        }
        xnioWorker.shutdown();
    }

    private void sendRequest(final ClientConnection connection, final AtomicInteger numRequestsRef, long startLoad) {
        // abort if expired
        if (System.currentTimeMillis() - startLoad > (MIN_TOTAL_DURATION_SEC * 1000)) return;

        requestsTotal.incrementAndGet();
        try {
            // prepare request
            ClientRequest request = new ClientRequest().setPath(URI.getPath()).setMethod(Methods.GET);
            request.getRequestHeaders().put(Headers.HOST, URI.getHost());

            // send request
            final var responseRef = new AtomicReference<ClientResponse>(null);
            CountDownLatch latch = new CountDownLatch(1);

            connection.sendRequest(request, createClientCallback(responseRef, latch));

            try {
                latch.await();
            } catch (InterruptedException e) {
                LOGGER.error(e.getMessage(), e);
            }

            // get response
            long timeout = System.currentTimeMillis();
            while (responseRef.get() == null && System.currentTimeMillis() - timeout < REQUEST_TIMEOUT) {
                TimeUnit.MICROSECONDS.sleep(10);
            }
            if (responseRef.get() != null) {
                if (LOGGER.isDebugEnabled()) {
                    final ClientResponse response = responseRef.get();
                    String protocolVersion = response.getProtocol().toString();
                    if ("HTTP/2.0".equals(protocolVersion)) LOGGER.info(response.getAttachment(RESPONSE_BODY));
                }
            } else {
                LOGGER.error("Request timeout");
            }

        } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
        } finally {
            numRequestsRef.decrementAndGet();
        }
    }

    private ClientCallback<ClientExchange> createClientCallback(final AtomicReference<ClientResponse> response, final CountDownLatch latch) {
        return new ClientCallback<>() {
            @Override
            public void completed(ClientExchange result) {
                result.setResponseListener(new ClientCallback<>() {
                    @Override
                    public void completed(final ClientExchange result) {
                        response.set(result.getResponse());
                        new StringReadChannelListener(result.getConnection().getBufferPool()) {

                            @Override
                            protected void stringDone(String string) {
                                result.getResponse().putAttachment(RESPONSE_BODY, string);
                            }

                            @Override
                            protected void error(IOException e) { }
                        }.setup(result.getResponseChannel());
                        latch.countDown();
                    }

                    @Override
                    public void failed(IOException e) {
                        latch.countDown();
                    }
                });
                try {
                    result.getRequestChannel().shutdownWrites();
                    if (!result.getRequestChannel().flush()) {
                        result.getRequestChannel().getWriteSetter().set(ChannelListeners.<StreamSinkChannel>flushingChannelListener(null, null));
                        result.getRequestChannel().resumeWrites();
                    }
                } catch (IOException ignore) { }
            }

            @Override
            public void failed(IOException e) {
                latch.countDown();
            }
        };
    }

}
