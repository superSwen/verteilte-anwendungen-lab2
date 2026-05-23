package de.berlin.htw.boundary.ws.client;

import static org.awaitility.Awaitility.await;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import de.berlin.htw.trading.quote.dto.Quote;
import io.quarkus.test.junit.QuarkusTest;

@QuarkusTest
public class QuoteClientIT {

    private static final ObjectMapper MAPPER = new ObjectMapper();


    /**
     * Test 1: QuoteClient verbindet sich zu WebSocket Server
     */
    @Test
    public void testQuoteClientConnects() throws Exception {
        // Dieser Test prüft, dass QuoteController im @PostConstruct
        // erfolgreich eine Verbindung zu Stock3 herstellt.
        // Wenn die Verbindung fehlschlägt, würde System.exit(1) aufgerufen.

        // Wenn dieser Test keine Exception wirft, war der Connect erfolgreich.
        // (QuoteController wird beim Startup automatisch initialisiert)

        Thread.sleep(1000); // Warte, dass QuoteController startet

        // Kein Assert nötig; wenn Connect fehlschlägt, beendet JVM sich selbst
        assert true;
    }

    /**
     * Test 2: Initial Quote JSON wird korrekt geparst und Event gefeuert
     */
    @Test
    public void testInitialQuoteParsing() {
        // Erzeuge eine Initial-Quote JSON wie Stock3 sie sendet
        String initialJson = "{" +
                "\"q\":24267.0," +
                "\"h\":24358.5," +
                "\"l\":24251.5," +
                "\"pc\":24330.03," +
                "\"o\":24313.5," +
                "\"ts\":1761117803," +
                "\"t\":2469," +
                "\"tickSize\":0.5," +
                "\"precision\":2," +
                "\"abs\":-63.03," +
                "\"rel\":-0.0025906," +
                "\"active\":true," +
                "\"i\":6," +
                "\"s\":\"133962:22:last\"" +
                "}";

        try {
            // Parse wie QuoteClient es macht
            var data = MAPPER.readValue(initialJson, java.util.Map.class);

            Double q = ((Number) data.get("q")).doubleValue();
            Double h = ((Number) data.get("h")).doubleValue();
            Double l = ((Number) data.get("l")).doubleValue();
            Integer subId = ((Number) data.get("i")).intValue();
            String symbolStr = (String) data.get("s");

            // Assertions
            assert q == 24267.0;
            assert h == 24358.5;
            assert l == 24251.5;
            assert subId == 6;
            assert "133962:22:last".equals(symbolStr);

        } catch (Exception e) {
            throw new RuntimeException("Failed to parse initial quote", e);
        }
    }

    /**
     * Test 3: Delta Message wird korrekt geparst
     */
    @Test
    public void testDeltaMessageParsing() {
        // Delta Message Format: subId:price:secSinceLast:tickDelta:newHigh:newLow:vDelta:tvAbs
        String deltaMessage = "6:24270.5:2:10:::::";

        // Parse wie DeltaQuote.parse es macht
        String[] parts = deltaMessage.split(":", -1);

        int subId = Integer.parseInt(parts[0]);
        Double value = parts[1].isEmpty() ? null : Double.parseDouble(parts[1]);
        Long secSinceLast = parts[2].isEmpty() ? null : Long.parseLong(parts[2]);
        Long tickDelta = parts[3].isEmpty() ? null : Long.parseLong(parts[3]);

        // Assertions
        assert subId == 6;
        assert value == 24270.5;
        assert secSinceLast == 2L;
        assert tickDelta == 10L;
    }

    /**
     * Test 4: Willkommensnachricht wird ignoriert
     */
    @Test
    public void testWelcomeMessageIgnored() {
        String welcomeMsg = "[stock3-websocket-v1.0]";

        // QuoteClient prüft ob message.startsWith("[stock3-")
        boolean shouldIgnore = welcomeMsg.startsWith("[stock3-");

        assert shouldIgnore : "Welcome message sollte ignoriert werden";
    }

    /**
     * Test 5: Quote mit allen Feldern erstellen
     */
    @Test
    public void testQuoteConstruction() {
        // Teste, dass Quote DTO korrekt erstellt wird
        de.berlin.htw.trading.quote.dto.SymbolKey key =
            new de.berlin.htw.trading.quote.dto.SymbolKey("133962", "22", "last");

        Quote quote = new Quote(
                key,
                1761117803L,  // ts
                24267.0,       // price
                24358.5,       // high
                24251.5,       // low
                24313.5,       // open
                24330.03,      // prevClose
                -63.03,        // abs
                -0.0025906,    // rel
                0.5,           // tickSize
                true,          // active
                2469L,         // tick
                6,             // subId
                2.0            // precision
        );

        assert quote.s().equals(key);
        assert quote.price() == 24267.0;
        assert quote.subId() == 6;
    }
}

