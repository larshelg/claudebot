package com.example.flink.tradeengine;

import com.example.flink.domain.ExecReport;
import com.example.flink.domain.TradeMatch;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.junit.ClassRule;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.jupiter.api.Assertions.*;

@SuppressWarnings("deprecation")
public class TradeMatchingJobIntegrationTest {

    @ClassRule
    static final MiniClusterWithClientResource flinkCluster = new MiniClusterWithClientResource(
            new MiniClusterResourceConfiguration.Builder()
                    .setNumberSlotsPerTaskManager(2)
                    .setNumberTaskManagers(1)
                    .build());

    // Test sink for TradeMatch results
    public static class TestTradeMatchSink implements Sink<TradeMatch> {
        private static final Queue<TradeMatch> collectedTradeMatches = new ConcurrentLinkedQueue<>();

        public static void clear() {
            collectedTradeMatches.clear();
        }

        public static List<TradeMatch> getResults() {
            return new ArrayList<>(collectedTradeMatches);
        }

        @Override
        public SinkWriter<TradeMatch> createWriter(InitContext context) {
            return new SinkWriter<TradeMatch>() {
                @Override
                public void write(TradeMatch element, Context context) {
                    collectedTradeMatches.add(copyTradeMatch(element));
                }

                @Override
                public void flush(boolean endOfInput) {
                }

                @Override
                public void close() {
                }
            };
        }

        private TradeMatch copyTradeMatch(TradeMatch match) {
            TradeMatch copy = new TradeMatch();
            copy.matchId = match.matchId;
            copy.accountId = match.accountId;
            copy.symbol = match.symbol;
            copy.buyOrderId = match.buyOrderId;
            copy.sellOrderId = match.sellOrderId;
            copy.matchedQty = match.matchedQty;
            copy.buyPrice = match.buyPrice;
            copy.sellPrice = match.sellPrice;
            copy.realizedPnl = match.realizedPnl;
            copy.matchTimestamp = match.matchTimestamp;
            copy.buyTimestamp = match.buyTimestamp;
            copy.sellTimestamp = match.sellTimestamp;
            return copy;
        }
    }

    // Helper method for creating ExecReport test data
    private static ExecReport createExecReport(String accountId, String orderId, String symbol,
            double fillQty, double fillPrice, String status, long timestamp) {
        return new ExecReport(accountId, orderId, symbol, fillQty, fillPrice, status, timestamp);
    }

    // Helper method to create ExecReport stream with watermarks
    private static DataStream<ExecReport> createExecReportStream(StreamExecutionEnvironment env,
            List<ExecReport> data) {
        DataStream<ExecReport> base = env.fromCollection(data);
        return base.assignTimestampsAndWatermarks(
                WatermarkStrategy.<ExecReport>forBoundedOutOfOrderness(
                        Duration.ofSeconds(1))
                        .withTimestampAssigner((e, ts) -> e.ts));
    }

    @Test
    public void testTradeMatchingJobWithSimpleScenario() throws Exception {
        // Clear test sink
        TestTradeMatchSink.clear();

        // Create environment
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        var baseTime = System.currentTimeMillis();

        // Simple buy-sell scenario
        var execReports = Arrays.asList(
                createExecReport("ACC_TM", "BUY001", "BTCUSD", 1.0, 50000.0, "FILLED", baseTime + 1000),
                createExecReport("ACC_TM", "SELL001", "BTCUSD", -0.5, 52000.0, "FILLED", baseTime + 2000)
        );

        // Create exec report stream
        var execReportStream = createExecReportStream(env, execReports);

        // Create and run TradeMatchingJob
        var tradeMatchingJob = new TradeMatchingJob(execReportStream, new TestTradeMatchSink());
        tradeMatchingJob.run();

        // Execute
        env.execute("TradeMatching Job Simple Test");

        // Validate results
        var tradeMatches = TestTradeMatchSink.getResults();
        assertEquals(1, tradeMatches.size(), "Should have exactly 1 trade match");

        var match = tradeMatches.get(0);
        assertEquals("ACC_TM", match.accountId);
        assertEquals("BTCUSD", match.symbol);
        assertEquals("BUY001", match.buyOrderId);
        assertEquals("SELL001", match.sellOrderId);
        assertEquals(0.5, match.matchedQty, 0.001);
        assertEquals(50000.0, match.buyPrice, 0.01);
        assertEquals(52000.0, match.sellPrice, 0.01);
        assertEquals(1000.0, match.realizedPnl, 0.01); // (52000 - 50000) * 0.5

        System.out.println("Simple Trade Matching Test Results:");
        System.out.printf("Match: %s->%s, Qty: %.2f, P&L: $%.2f%n",
                match.buyOrderId, match.sellOrderId, match.matchedQty, match.realizedPnl);
    }

    @Test
    public void testTradeMatchingJobWithComplexFIFO() throws Exception {
        // Clear test sink
        TestTradeMatchSink.clear();

        // Create environment
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        var baseTime = System.currentTimeMillis();

        // Complex FIFO scenario matching our earlier test
        var execReports = Arrays.asList(
                // Build position with multiple buys
                createExecReport("ACC_FIFO", "BUY001", "BTCUSD", 1.0, 50000.0, "FILLED", baseTime + 1000),
                createExecReport("ACC_FIFO", "BUY002", "BTCUSD", 0.5, 52000.0, "FILLED", baseTime + 2000),
                createExecReport("ACC_FIFO", "BUY003", "BTCUSD", 2.0, 48000.0, "FILLED", baseTime + 3000),
                
                // Large sell that spans multiple buys
                createExecReport("ACC_FIFO", "SELL001", "BTCUSD", -2.2, 51000.0, "FILLED", baseTime + 4000)
        );

        // Create exec report stream
        var execReportStream = createExecReportStream(env, execReports);

        // Create and run TradeMatchingJob
        var tradeMatchingJob = new TradeMatchingJob(execReportStream, new TestTradeMatchSink());
        tradeMatchingJob.run();

        // Execute
        env.execute("TradeMatching Job FIFO Test");

        // Validate FIFO results
        var tradeMatches = TestTradeMatchSink.getResults().stream()
                .filter(match -> "ACC_FIFO".equals(match.accountId))
                .sorted((a, b) -> Long.compare(a.buyTimestamp, b.buyTimestamp)) // Sort by buy time to verify FIFO
                .toList();

        assertEquals(3, tradeMatches.size(), "Should have 3 matches (SELL spans 3 BUY orders)");

        // First match: SELL001 vs BUY001 (oldest buy)
        var match1 = tradeMatches.get(0);
        assertEquals("BUY001", match1.buyOrderId, "First match should be against BUY001 (FIFO)");
        assertEquals(1.0, match1.matchedQty, 0.001, "Should fully consume BUY001");
        assertEquals(1000.0, match1.realizedPnl, 0.01, "(51000-50000)*1.0 = 1000");

        // Second match: SELL001 vs BUY002
        var match2 = tradeMatches.get(1);
        assertEquals("BUY002", match2.buyOrderId, "Second match should be against BUY002");
        assertEquals(0.5, match2.matchedQty, 0.001, "Should fully consume BUY002");
        assertEquals(-500.0, match2.realizedPnl, 0.01, "(51000-52000)*0.5 = -500");

        // Third match: SELL001 vs BUY003 (partial)
        var match3 = tradeMatches.get(2);
        assertEquals("BUY003", match3.buyOrderId, "Third match should be against BUY003");
        assertEquals(0.7, match3.matchedQty, 0.001, "Should partially consume BUY003");
        assertEquals(2100.0, match3.realizedPnl, 0.01, "(51000-48000)*0.7 = 2100");

        // Total P&L should be sum of all matches
        double totalPnl = tradeMatches.stream().mapToDouble(m -> m.realizedPnl).sum();
        assertEquals(2600.0, totalPnl, 0.01, "Total P&L should be 1000 - 500 + 2100 = 2600");

        System.out.println("Complex FIFO Trade Matching Test Results:");
        System.out.println("Total matches: " + tradeMatches.size());
        System.out.println("Total P&L: $" + totalPnl);
        tradeMatches.forEach(match -> System.out.printf(
                "Match: %s->SELL001, Qty: %.2f, Buy@%.2f, P&L: $%.2f%n",
                match.buyOrderId, match.matchedQty, match.buyPrice, match.realizedPnl));
    }

    @Test
    public void testTradeMatchingJobWithMultipleSymbols() throws Exception {
        // Clear test sink
        TestTradeMatchSink.clear();

        // Create environment
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        var baseTime = System.currentTimeMillis();

        // Multiple symbols - should be isolated
        var execReports = Arrays.asList(
                // BTC trades
                createExecReport("ACC_MULTI", "BUY_BTC", "BTCUSD", 1.0, 50000.0, "FILLED", baseTime + 1000),
                createExecReport("ACC_MULTI", "SELL_BTC", "BTCUSD", -0.8, 52000.0, "FILLED", baseTime + 2000),
                
                // ETH trades (should not match with BTC)
                createExecReport("ACC_MULTI", "BUY_ETH", "ETHUSD", 10.0, 3000.0, "FILLED", baseTime + 1500),
                createExecReport("ACC_MULTI", "SELL_ETH", "ETHUSD", -5.0, 3200.0, "FILLED", baseTime + 2500)
        );

        // Create exec report stream
        var execReportStream = createExecReportStream(env, execReports);

        // Create and run TradeMatchingJob
        var tradeMatchingJob = new TradeMatchingJob(execReportStream, new TestTradeMatchSink());
        tradeMatchingJob.run();

        // Execute
        env.execute("TradeMatching Job Multi-Symbol Test");

        // Validate results
        var tradeMatches = TestTradeMatchSink.getResults().stream()
                .filter(match -> "ACC_MULTI".equals(match.accountId))
                .toList();

        assertEquals(2, tradeMatches.size(), "Should have 2 matches (1 BTC, 1 ETH)");

        // Verify BTC match
        var btcMatch = tradeMatches.stream()
                .filter(match -> "BTCUSD".equals(match.symbol))
                .findFirst().orElse(null);
        assertNotNull(btcMatch, "Should have BTC trade match");
        assertEquals(0.8, btcMatch.matchedQty, 0.001);
        assertEquals(1600.0, btcMatch.realizedPnl, 0.01); // (52000-50000)*0.8

        // Verify ETH match
        var ethMatch = tradeMatches.stream()
                .filter(match -> "ETHUSD".equals(match.symbol))
                .findFirst().orElse(null);
        assertNotNull(ethMatch, "Should have ETH trade match");
        assertEquals(5.0, ethMatch.matchedQty, 0.001);
        assertEquals(1000.0, ethMatch.realizedPnl, 0.01); // (3200-3000)*5.0

        System.out.println("Multi-Symbol Trade Matching Test Results:");
        tradeMatches.forEach(match -> System.out.printf(
                "Match: %s, Qty: %.2f, P&L: $%.2f%n",
                match.symbol, match.matchedQty, match.realizedPnl));
    }
}