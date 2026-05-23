package de.berlin.htw.boundary.ws;

import static org.awaitility.Awaitility.await;

import java.net.URI;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import de.berlin.htw.boundary.ws.dto.WsMsgs;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.json.bind.Jsonb;
import jakarta.json.bind.JsonbBuilder;
import jakarta.websocket.ContainerProvider;
import jakarta.websocket.Session;
import jakarta.websocket.WebSocketContainer;

@QuarkusTest
public class WebsocketServerIT {

    private static Jsonb jsonb = JsonbBuilder.create();

    /**
     * Test 1: Client sollte sich verbinden können und onOpen wird aufgerufen
     */
    @Test
    public void testClientConnect() throws Exception {
        WebSocketContainer container = ContainerProvider.getWebSocketContainer();
        AtomicReference<String> receivedMsg = new AtomicReference<>();

        SimpleWebSocketClient client = new SimpleWebSocketClient() {
            @Override
            public void onMessage(String message) {
                receivedMsg.set(message);
            }
        };

        Session session = container.connectToServer(client,
                URI.create("ws://localhost:8080/quotes"));

        // Warte kurz, damit Server Zeit hat zu reagieren
        Thread.sleep(500);

        session.close();

        // Keine Fehler beim Connect → Test erfolgreich
        assert session != null;
    }

    /**
     * Test 2: Client sendet subscribe → Server sendet initiale Daten zurück
     */
    @Test
    public void testSubscribeReceivesData() throws Exception {
        WebSocketContainer container = ContainerProvider.getWebSocketContainer();
        AtomicReference<String> receivedMsg = new AtomicReference<>();

        SimpleWebSocketClient client = new SimpleWebSocketClient() {
            @Override
            public void onMessage(String message) {
                receivedMsg.set(message);
            }
        };

        Session session = container.connectToServer(client,
                URI.create("ws://localhost:8080/quotes"));

        // Sende subscribe Nachricht
        WsMsgs.Sub sub = new WsMsgs.Sub();
        sub.action = "subscribe";
        sub.symbolId = "133962";
        sub.venueId = "22";
        sub.channel = "last";
        sub.window = 3600;

        String json = jsonb.toJson(sub);
        session.getAsyncRemote().sendText(json);

        // Warte auf Response
        await()
                .atMost(5, TimeUnit.SECONDS)
                .until(() -> receivedMsg.get() != null);

        // Response sollte "type" enthalten
        assert receivedMsg.get() != null;
        assert receivedMsg.get().contains("\"type\"");

        session.close();
    }

    /**
     * Test 3: Client sendet ping → Server antwortet mit pong
     */
    @Test
    public void testPingPong() throws Exception {
        WebSocketContainer container = ContainerProvider.getWebSocketContainer();
        AtomicReference<String> receivedMsg = new AtomicReference<>();

        SimpleWebSocketClient client = new SimpleWebSocketClient() {
            @Override
            public void onMessage(String message) {
                receivedMsg.set(message);
            }
        };

        Session session = container.connectToServer(client,
                URI.create("ws://localhost:8080/quotes"));

        // Sende ping (unbekannte action)
        String pingMsg = "{\"type\":\"ping\"}";
        session.getAsyncRemote().sendText(pingMsg);

        // Warte auf Pong-Response
        await()
                .atMost(5, TimeUnit.SECONDS)
                .until(() -> receivedMsg.get() != null);

        // Response sollte pong sein
        assert receivedMsg.get() != null;
        assert receivedMsg.get().contains("\"type\":\"pong\"");

        session.close();
    }
}

/**
 * Einfacher WebSocket-Client für Tests
 */
@jakarta.websocket.ClientEndpoint
abstract class SimpleWebSocketClient {
    @jakarta.websocket.OnMessage
    public abstract void onMessage(String message);

    @jakarta.websocket.OnError
    public void onError(Throwable t) {
        System.err.println("WebSocket error: " + t.getMessage());
    }
}

