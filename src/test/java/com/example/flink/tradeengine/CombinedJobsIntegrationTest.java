package com.example.flink.tradeengine;

import com.example.flink.exchange.LocalTestOrderExecutionJob;
import com.example.flink.StrategySignal;
import com.example.flink.domain.AccountPolicy;
import com.example.flink.domain.Position;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.junit.ClassRule;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;

import static org.junit.jupiter.api.Assertions.*;
import com.example.flink.strategyengine.StrategyChooserJob;

public class CombinedJobsIntegrationTest {

    @ClassRule
    static final MiniClusterWithClientResource flinkCluster = new MiniClusterWithClientResource(
            new MiniClusterResourceConfiguration.Builder()
                    .setNumberSlotsPerTaskManager(2)
                    .setNumberTaskManagers(1)
                    .build());

    @Test
    public void testStrategyChooserAndPortfolioJobsWithSharedPositionSink() throws Exception {
        // Clear test sinks
        TestUpsertSinks.PositionLatestSink.clear();

        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        var baseTime = System.currentTimeMillis();

        // Strategy signals (BUY 1 BTCUSD). Keep a single BUY so position remains open.
        var strategySignals = Arrays.asList(
                new StrategySignal("ACC_INT", "BTCUSD", baseTime + 1000, 100.0, 99.0, 101.0, "BUY", 1.0));

        // Account policy
        var policies = Collections.singletonList(
                new AccountPolicy("ACC_INT", 3, "ACTIVE", 100_000.0, baseTime));

        // Build streams
        var strategySignalStream = env.fromCollection(strategySignals)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<StrategySignal>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                                .withTimestampAssigner((s, ts) -> s.timestamp));

        var policyStream = env.fromCollection(policies)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<AccountPolicy>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                                .withTimestampAssigner((p, ts) -> p.ts));

        var emptyPositions = env.fromElements(new Position())
                .filter(p -> false);

        // No initial exec reports; LocalTestOrderExecutionJob will produce them from
        // accepted trades

        // Create StrategyChooser pipeline in test mode and capture accepted trades
        var chooserResults = StrategyChooserJob.processStrategySignals(
                strategySignalStream,
                policyStream,
                emptyPositions,
                null);
        var acceptedTrades = chooserResults.accepted;
        var execReportsFromLocal = LocalTestOrderExecutionJob.processTradeSignals(acceptedTrades, null);

        // Build portfolio/risk pipeline in test mode and attach test sinks
        var results = PortfolioAndRiskJob.processExecReports(execReportsFromLocal, policyStream, null);
        results.positions.sinkTo(new TestUpsertSinks.PositionLatestSink());

        env.execute("Combined Jobs Integration Test");

        // Verify an open position exists for ACC_INT/BTCUSD (qty 1)
        var pos = TestUpsertSinks.PositionLatestSink.get("ACC_INT", "BTCUSD");
        assertNotNull(pos, "Expected open position for ACC_INT/BTCUSD");
        assertEquals(1.0, pos.netQty, 1e-6);
    }

    @Test
    public void testSellThenBuyPositionFlow() throws Exception {
        // Clear test sinks
        TestUpsertSinks.PositionLatestSink.clear();

        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        var baseTime = System.currentTimeMillis();

        // Strategy signals: SELL first (short position), then BUY (close position)
        var strategySignals = Arrays.asList(
                new StrategySignal("ACC_SHORT", "ETHUSD", baseTime + 1000, 2500.0, 2480.0, 2520.0, "SELL", 0.8),
                new StrategySignal("ACC_SHORT", "ETHUSD", baseTime + 2000, 2480.0, 2460.0, 2500.0, "BUY", 0.6));

        // Account policy
        var policies = Collections.singletonList(
                new AccountPolicy("ACC_SHORT", 5, "ACTIVE", 50_000.0, baseTime));

        // Build streams
        var strategySignalStream = env.fromCollection(strategySignals)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<StrategySignal>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                                .withTimestampAssigner((s, ts) -> s.timestamp));

        var policyStream = env.fromCollection(policies)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<AccountPolicy>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                                .withTimestampAssigner((p, ts) -> p.ts));

        var emptyPositions = env.fromElements(new Position())
                .filter(p -> false);

        // Create StrategyChooser pipeline and capture accepted trades
        var chooserResults = StrategyChooserJob.processStrategySignals(
                strategySignalStream,
                policyStream,
                emptyPositions,
                null);
        var acceptedTrades = chooserResults.accepted;
        var execReportsFromLocal = LocalTestOrderExecutionJob.processTradeSignals(acceptedTrades, null);

        // Build portfolio/risk pipeline and attach test sinks
        var results = PortfolioAndRiskJob.processExecReports(execReportsFromLocal, policyStream, null);
        results.positions.sinkTo(new TestUpsertSinks.PositionLatestSink());

        env.execute("Sell Then Buy Position Flow Test");

        // Verify final position is closed (removed from sink due to netQty = 0)
        var pos = TestUpsertSinks.PositionLatestSink.get("ACC_SHORT", "ETHUSD");
        assertNull(pos, "Position should be closed and removed when SELL(-1) + BUY(+1) = 0");
        
        // Verify no open positions remain
        var allPositions = TestUpsertSinks.PositionLatestSink.getResults();
        assertTrue(allPositions.isEmpty(), "No positions should remain open");
        
        System.out.println("Successfully tested SELL -> BUY flow: position opened short and then closed");
    }

    @Test
    public void testShortPositionOnly() throws Exception {
        // Clear test sinks
        TestUpsertSinks.PositionLatestSink.clear();

        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        var baseTime = System.currentTimeMillis();

        // Strategy signals: SELL only (keep short position open)
        var strategySignals = Arrays.asList(
                new StrategySignal("ACC_SHORT_ONLY", "BTCUSD", baseTime + 1000, 45000.0, 44800.0, 45200.0, "SELL", 0.9));

        // Account policy
        var policies = Collections.singletonList(
                new AccountPolicy("ACC_SHORT_ONLY", 3, "ACTIVE", 100_000.0, baseTime));

        // Build streams
        var strategySignalStream = env.fromCollection(strategySignals)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<StrategySignal>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                                .withTimestampAssigner((s, ts) -> s.timestamp));

        var policyStream = env.fromCollection(policies)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<AccountPolicy>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                                .withTimestampAssigner((p, ts) -> p.ts));

        var emptyPositions = env.fromElements(new Position())
                .filter(p -> false);

        // Create StrategyChooser pipeline and capture accepted trades
        var chooserResults = StrategyChooserJob.processStrategySignals(
                strategySignalStream,
                policyStream,
                emptyPositions,
                null);
        var acceptedTrades = chooserResults.accepted;
        var execReportsFromLocal = LocalTestOrderExecutionJob.processTradeSignals(acceptedTrades, null);

        // Build portfolio/risk pipeline and attach test sinks
        var results = PortfolioAndRiskJob.processExecReports(execReportsFromLocal, policyStream, null);
        results.positions.sinkTo(new TestUpsertSinks.PositionLatestSink());

        env.execute("Short Position Only Test");

        // Verify short position exists (netQty = -1)
        var pos = TestUpsertSinks.PositionLatestSink.get("ACC_SHORT_ONLY", "BTCUSD");
        assertNotNull(pos, "Expected short position for ACC_SHORT_ONLY/BTCUSD");
        assertEquals(-1.0, pos.netQty, 1e-6, "Should have short position of -1");
        assertEquals(45000.0, pos.avgPrice, 1e-6, "Average price should match fill price");
        
        System.out.println("Short position created: netQty=" + pos.netQty + ", avgPrice=" + pos.avgPrice);
    }

    @Test
    public void testMaxOpenPositionsLimitEnforced() throws Exception {
        TestUpsertSinks.PositionLatestSink.clear();

        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        var baseTime = System.currentTimeMillis();

        // Four BUY signals on distinct symbols for same account
        var strategySignals = Arrays.asList(
                new StrategySignal("ACC_MAX", "SYM1", baseTime + 1000, 100.0, 99.0, 101.0, "BUY", 1.0),
                new StrategySignal("ACC_MAX", "SYM2", baseTime + 1100, 100.0, 99.0, 101.0, "BUY", 1.0),
                new StrategySignal("ACC_MAX", "SYM3", baseTime + 1200, 100.0, 99.0, 101.0, "BUY", 1.0),
                new StrategySignal("ACC_MAX", "SYM4", baseTime + 1300, 100.0, 99.0, 101.0, "BUY", 1.0));

        // Policy allows max 3 open symbols
        var policies = Collections.singletonList(
                new AccountPolicy("ACC_MAX", 3, "ACTIVE", 100_000.0, baseTime));

        var strategySignalStream = env.fromCollection(strategySignals)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<StrategySignal>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                                .withTimestampAssigner((s, ts) -> s.timestamp));

        var policyStream = env.fromCollection(policies)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<AccountPolicy>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                                .withTimestampAssigner((p, ts) -> p.ts));

        var emptyPositions = env.fromElements(new Position())
                .filter(p -> false);

        // Run chooser to gate trades by policy/positions (test mode)
        var chooserResults = StrategyChooserJob.processStrategySignals(
                strategySignalStream,
                policyStream,
                emptyPositions,
                null);
        var acceptedTrades = chooserResults.accepted;
        var execReportsFromLocal = LocalTestOrderExecutionJob.processTradeSignals(acceptedTrades, null);

        // Portfolio job processes exec reports in test mode; attach test sink
        var results = PortfolioAndRiskJob.processExecReports(execReportsFromLocal, policyStream, null);
        results.positions.sinkTo(new TestUpsertSinks.PositionLatestSink());

        env.execute("Max Open Positions Limit Test");

        // Verify only 3 open positions for ACC_MAX
        var positions = TestUpsertSinks.PositionLatestSink.getResults();
        var count = positions.stream()
                .filter(p -> "ACC_MAX".equals(p.accountId))
                .map(p -> p.symbol)
                .distinct()
                .count();
        assertEquals(3L, count, "Should cap at 3 open symbols");
    }

    @Test
    public void testLoadAllStrategySignalsFromFile() throws Exception {
        // Clear test sinks
        TestUpsertSinks.PositionLatestSink.clear();

        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Load strategy signals from JSON file
        var strategySignals = loadStrategySignalsFromJson();
        System.out.println("Loaded " + strategySignals.size() + " strategy signals from file");

        // Account policy for live account
        var policies = Collections.singletonList(
                new AccountPolicy("live", 10, "ACTIVE", 1_000_000.0, System.currentTimeMillis()));

        // Build streams
        var strategySignalStream = env.fromCollection(strategySignals)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<StrategySignal>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                                .withTimestampAssigner((s, ts) -> s.timestamp));

        var policyStream = env.fromCollection(policies)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<AccountPolicy>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                                .withTimestampAssigner((p, ts) -> p.ts));

        var emptyPositions = env.fromElements(new Position())
                .filter(p -> false);

        // Create StrategyChooser pipeline and capture accepted trades
        var chooserResults = StrategyChooserJob.processStrategySignals(
                strategySignalStream,
                policyStream,
                emptyPositions,
                null);
        var acceptedTrades = chooserResults.accepted;
        var execReportsFromLocal = LocalTestOrderExecutionJob.processTradeSignals(acceptedTrades, null);

        // Build portfolio/risk pipeline and attach test sinks
        var results = PortfolioAndRiskJob.processExecReports(execReportsFromLocal, policyStream, null);
        results.positions.sinkTo(new TestUpsertSinks.PositionLatestSink());

        // Add trade match sink to capture trade matching results
        TestUpsertSinks.TradeMatchHistorySink.clear();
        results.tradeMatches.sinkTo(new TestUpsertSinks.TradeMatchHistorySink());
        
        // Add position close history sink
        TestUpsertSinks.PositionCloseHistorySink.clear();
        results.positionCloses.sinkTo(new TestUpsertSinks.PositionCloseHistorySink());

        env.execute("Load All Strategy Signals Test");

        // Check trade matches first
        var tradeMatches = TestUpsertSinks.TradeMatchHistorySink.getResults();
        System.out.println("Trade matches found: " + tradeMatches.size());
        tradeMatches.forEach(tm -> System.out.println("  Trade match: " + tm.accountId + "/" + tm.symbol + 
                                                     " - Qty: " + tm.matchedQty + ", Buy: " + tm.buyPrice + ", Sell: " + tm.sellPrice + ", PnL: " + tm.realizedPnl));

        // Check position close history
        var positionCloses = TestUpsertSinks.PositionCloseHistorySink.getResults();
        System.out.println("Position closes found: " + positionCloses.size());
        positionCloses.forEach(pc -> System.out.println("  Position close: " + pc.accountId + "/" + pc.symbol + 
                                                        " - TotalQty: " + pc.totalQty + ", AvgPrice: " + pc.avgPrice + 
                                                        ", Duration: " + (pc.closeTs - pc.openTs) + "ms, PnL: " + pc.realizedPnl));

        // Check if any positions exist (may be empty if final position nets to 0)
        var positions = TestUpsertSinks.PositionLatestSink.getResults();
        System.out.println("All positions found:");
        positions.forEach(p -> System.out.println("  Account: " + p.accountId + ", Symbol: " + p.symbol + ", NetQty: " + p.netQty));
        
        // Count BUY vs SELL signals to determine expected final position
        int buyCount = (int) strategySignals.stream().filter(s -> "BUY".equals(s.signal)).count();
        int sellCount = (int) strategySignals.stream().filter(s -> "SELL".equals(s.signal)).count();
        double expectedNetQty = buyCount - sellCount;
        
        System.out.println("BUY signals: " + buyCount + ", SELL signals: " + sellCount + 
                          ", Expected net qty: " + expectedNetQty);
        
        if (expectedNetQty == 0) {
            // Position should be removed from upsert sink when netted to zero
            var suiPosition = TestUpsertSinks.PositionLatestSink.get("live", "SUI_USDT");
            assertNull(suiPosition, "Position should be removed when netQty = 0");
            System.out.println("Position correctly removed when netQty = 0");
        } else {
            // Position should exist with expected netQty
            var suiPosition = TestUpsertSinks.PositionLatestSink.get("live", "SUI_USDT");
            assertNotNull(suiPosition, "Expected position for live/SUI_USDT");
            assertEquals(expectedNetQty, suiPosition.netQty, 1e-6, "Net quantity should match");
            System.out.println("Final SUI_USDT position: " + suiPosition.netQty);
        }
    }

    private List<StrategySignal> loadStrategySignalsFromJson() throws Exception {
        String filePath = "src/test/java/com/example/flink/tradeengine/signals.json";
        String content = Files.readString(Paths.get(filePath));
        
        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonArray = mapper.readTree(content);
        
        List<StrategySignal> signals = new ArrayList<>();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
        
        for (JsonNode node : jsonArray) {
            String runId = node.get("run_id").asText();
            String symbol = node.get("symbol").asText();
            String timeStr = node.get("time").asText();
            double close = node.get("close").asDouble();
            double sma5 = node.get("sma5").asDouble();
            double sma21 = node.get("sma21").asDouble();
            String signal = node.get("signal").asText();
            double signalStrength = node.get("signal_strength").asDouble();
            
            // Convert time string to timestamp
            LocalDateTime dateTime = LocalDateTime.parse(timeStr, formatter);
            long timestamp = dateTime.toEpochSecond(ZoneOffset.UTC) * 1000;
            
            signals.add(new StrategySignal(runId, symbol, timestamp, close, sma5, sma21, signal, signalStrength));
        }
        
        return signals;
    }
}
