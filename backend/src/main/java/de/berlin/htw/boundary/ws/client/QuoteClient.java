package de.berlin.htw.boundary.ws.client;

import jakarta.websocket.ClientEndpoint;
import jakarta.websocket.OnOpen;
import jakarta.websocket.OnMessage;
import jakarta.websocket.OnError;
import jakarta.websocket.Session;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.jboss.logging.Logger;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.berlin.htw.trading.events.InitialQuoteEvent;
import de.berlin.htw.trading.events.QuoteDeltaEvent;
import de.berlin.htw.trading.quote.dto.DeltaQuote;
import de.berlin.htw.trading.quote.dto.Quote;
import de.berlin.htw.trading.quote.dto.SymbolKey;
import jakarta.enterprise.event.Event;
import jakarta.inject.Inject;
import io.quarkus.scheduler.Scheduled;

@ClientEndpoint
public class QuoteClient {

    @Inject
    Logger logger;

    @Inject
    private Event<InitialQuoteEvent> quoteEvent;

    @Inject
    private Event<QuoteDeltaEvent> quoteDeltaEvent;

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final Map<Integer, SymbolKey> subMap = new ConcurrentHashMap<>();
    private Session session;


    @OnOpen
    public void onOpen(Session session) {
        logger.info("Connected to Stock3 WebSocket server");
        this.session = session;
    }

    @OnMessage
    public void onMessage(String message, Session session) {
        try {
            if (message.startsWith("[stock3-")) {
                logger.debug("Ignoring welcome message from Stock3");
                return;
            }

            if (message.startsWith("{")) {
                handleInitialQuote(message);
            }
            else {
                handleDeltaMessage(message);
            }
        } catch (Exception e) {
            logger.error("Error processing message from Stock3: " + e.getMessage(), e);
        }
    }

    @OnError
    public void onError(Session session, Throwable throwable) {
        logger.error("WebSocket error: " + throwable.getMessage(), throwable);
    }


    private void handleInitialQuote(String jsonMessage) throws Exception {


        Map<String, Object> data = MAPPER.readValue(jsonMessage, Map.class);

        // Extrahiere die Felder
        Double q = ((Number) data.get("q")).doubleValue();           // aktueller Kurs
        Double h = ((Number) data.get("h")).doubleValue();           // Tageshoch
        Double l = ((Number) data.get("l")).doubleValue();           // Tagestief
        Double pc = ((Number) data.get("pc")).doubleValue();         // Vortageskurs
        Double o = ((Number) data.get("o")).doubleValue();           // Eröffnungskurs
        Long ts = ((Number) data.get("ts")).longValue();             // Zeitstempel (Sekunden)
        Long t = ((Number) data.get("t")).longValue();               // Ticknummer
        Double tickSize = ((Number) data.get("tickSize")).doubleValue();
        Integer precision = ((Number) data.get("precision")).intValue();
        Double abs = ((Number) data.get("abs")).doubleValue();       // abs. Änderung zum Vortag
        Double rel = ((Number) data.get("rel")).doubleValue();       // rel. Änderung zum Vortag
        Boolean active = (Boolean) data.get("active");
        Integer subId = ((Number) data.get("i")).intValue();         // Subscription-ID
        String symbolStr = (String) data.get("s");                   // Symbol-String (z.B. "133962:22:last")

        SymbolKey key = parseSymbolKey(symbolStr);
        if (key == null) return;

        subMap.put(subId, key);

        Quote quote = new Quote(key, ts, q, h, l, o, pc, abs, rel, tickSize, active, t, subId, (double) precision);

        logger.infof("Initial quote received: %s at %.2f (subId=%d)", key, q, subId);
        quoteEvent.fireAsync(new InitialQuoteEvent(quote));
    }

    private void handleDeltaMessage(String deltaMessage) {


        DeltaQuote deltaQuote = DeltaQuote.parse(deltaMessage);

        logger.debugf("Delta message: subId=%d, price=%.4f",
            deltaQuote.subId(), deltaQuote.value());

        quoteDeltaEvent.fireAsync(new QuoteDeltaEvent(deltaQuote));
    }


    private SymbolKey parseSymbolKey(String symbolStr) {
        String[] parts = symbolStr.split(":");
        if (parts.length < 3) {
            logger.warn("Invalid symbol string: " + symbolStr);
            return null;
        }
        return new SymbolKey(parts[0], parts[1], parts[2]);
    }

}