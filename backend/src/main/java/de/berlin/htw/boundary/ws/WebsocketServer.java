package de.berlin.htw.boundary.ws;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.jboss.logging.Logger;

import de.berlin.htw.boundary.ws.dto.SubEvent;
import de.berlin.htw.boundary.ws.dto.Subscription;
import de.berlin.htw.boundary.ws.dto.UnsubEvent;
import de.berlin.htw.boundary.ws.dto.WsMsgs;
import de.berlin.htw.trading.candle.CandleQuoteConsumer;
import de.berlin.htw.trading.candle.dto.Candle;
import de.berlin.htw.trading.events.CandleEvent;
import de.berlin.htw.trading.events.QuoteEvent;
import de.berlin.htw.trading.quote.SimpleQuoteConsumer;
import de.berlin.htw.trading.quote.dto.SymbolKey;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Event;
import jakarta.enterprise.event.ObservesAsync;
import jakarta.inject.Inject;
import jakarta.json.bind.Jsonb;
import jakarta.json.bind.JsonbBuilder;
import jakarta.websocket.OnClose;
import jakarta.websocket.OnError;
import jakarta.websocket.OnMessage;
import jakarta.websocket.OnOpen;
import jakarta.websocket.Session;
import jakarta.websocket.server.ServerEndpoint;

@ServerEndpoint("/quotes")
@ApplicationScoped
public class WebsocketServer {

    private final Map<String, Session> sessions = new ConcurrentHashMap<>();

    private final Map<String, Subscription> subs = new ConcurrentHashMap<>();

    private final Jsonb jsonb = JsonbBuilder.create();

    @Inject
    private CandleQuoteConsumer candleQuoteConsumer;

    @Inject
    private SimpleQuoteConsumer simpleQuoteConsumer;

    @Inject
    private Event<SubEvent> subEvent;

    @Inject
    private Event<UnsubEvent> unsubEvent;

    @Inject
    Logger logger;

    @OnOpen
    public void onOpen(Session session) {
        sessions.put(session.getId(), session);
        subs.put(session.getId(), new Subscription());
        logger.infov("Client connected: {0}", session.getId());
    }

    @OnClose
    public void onClose(Session session) {
        sessions.remove(session.getId());
        subs.remove(session.getId());
        logger.infov("Client disconnected: {0}", session.getId());
    }

    @OnError
    public void onError(Session session, Throwable throwable) {
        logger.errorv("WebSocket error for session {0}: {1}", session.getId(), throwable.getMessage());
        sessions.remove(session.getId());
        subs.remove(session.getId());
    }

    @OnMessage
    public void onMessage(String message, Session session) {
        try {
            WsMsgs.Sub msg = jsonb.fromJson(message, WsMsgs.Sub.class);
            if ("subscribe".equals(msg.action)) {
                subscribe(msg, session);
            } else if ("unsubscribe".equals(msg.action)) {
                unsubscribe(msg, session);
            } else {
                // Unbekannte Nachricht (z.B. { type: "ping" }) → mit Pong antworten
                sendJson(session, new WsMsgs.Pong());
            }
        } catch (Exception e) {
            logger.errorv("Error processing message from session {0}: {1}", session.getId(), e.getMessage());
        }
    }

    void unsubscribe(WsMsgs.Sub sub, Session session) {
        SymbolKey key = toKey(sub);
        if (key == null)
            return;
        subs.get(session.getId()).windows.remove(key);
        subs.forEach((sid, s) -> {
            if (s.windows.keySet().stream().anyMatch(key::equals))
                return;
        });
        unsubEvent.fire(new UnsubEvent(key));
    }

    void subscribe(WsMsgs.Sub sub, Session session) {
        SymbolKey key = toKey(sub);
        if (key == null)
            return;
        subEvent.fire(new SubEvent(key));

        int windowSecs = (sub.window != null && sub.window > 0) ? sub.window : 3600;

        subs.get(session.getId()).windows.put(key, windowSecs);

        var list = candleQuoteConsumer.getCandles(key, Duration.ofSeconds(windowSecs));
        if (list == null || list.isEmpty()) {
            var lastKnownCandle = candleQuoteConsumer.getLastKnownCandle(key);
            if (lastKnownCandle != null) {
                list = List.of(lastKnownCandle);
            } else {
                list = List.of();
            }
        }
        var payload = Map.of("type", "candles", "key", key, "data", list);
        sendJson(session, payload);
        var quoteList = simpleQuoteConsumer.getQuotes(key, Duration.ofSeconds(windowSecs));
        if (quoteList == null || quoteList.isEmpty()) {
            var lastKnownQuote = simpleQuoteConsumer.getLastKnown(key);
            if (lastKnownQuote != null) {
                quoteList = List.of(lastKnownQuote);
            } else {
                quoteList = List.of();
            }
        }
        var quotePayload = Map.of("type", "quotes", "key", key,
                "data", quoteList);
        sendJson(session, quotePayload);
    }

    void onCandleEvent(@ObservesAsync CandleEvent ev) {
        SymbolKey key = ev.symbolKey();

        Candle c = candleQuoteConsumer.getLatestCandle(key);
        if (c == null)
            c = candleQuoteConsumer.getLastKnownCandle(key);
        if (c == null)
            return;

        var msg = Map.of("type", "candle", "key", key, "data", c);

        for (var entry : sessions.entrySet()) {
            var sessionId = entry.getKey();
            var session = entry.getValue();
            var st = subs.get(sessionId);
            if (st == null)
                continue;
            if (st.windows.keySet().stream().anyMatch(key::equals)) {
                sendJson(session, msg);
            }
        }
    }

    void onQuoteEvent(@ObservesAsync QuoteEvent ev) {
        SymbolKey key = ev.key();

        var q = simpleQuoteConsumer.getLast(key);
        if (q == null)
            q = simpleQuoteConsumer.getLastKnown(key);
        if (q == null)
            return;

        var msg = Map.of("type", "quote", "key", key, "data", q);

        for (var entry : sessions.entrySet()) {
            var sessionId = entry.getKey();
            var session = entry.getValue();
            var st = subs.get(sessionId);
            if (st == null)
                continue;
            if (st.windows.keySet().stream().anyMatch(key::equals)) {
                sendJson(session, msg);
            }
        }
    }

    private static SymbolKey toKey(WsMsgs.Sub sub) {
        if (sub.venueId == null || sub.symbolId == null)
            return null;
        return new SymbolKey(sub.symbolId, sub.venueId, sub.channel == null ? "last" : sub.channel);
    }

    private void sendJson(Session s, Object obj) {
        Session session = sessions.get(s.getId());
        if (session == null || !session.isOpen())
            return;
        try {
            String json = jsonb.toJson(obj);
            logger.infov("Sending JSON over WebSocket: {0}", json);
            session.getAsyncRemote().sendText(json);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
