package com.example.flink.tradeengine;

import com.example.flink.domain.AccountPolicy;
import com.example.flink.domain.ExecReport;
import com.example.flink.domain.Portfolio;
import com.example.flink.domain.Position;
import com.example.flink.domain.RiskAlert;
import com.example.flink.domain.TradeSignal;
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
public class PortfolioAndRiskJobIntegrationTest {

    @ClassRule
    static final MiniClusterWithClientResource flinkCluster = new MiniClusterWithClientResource(
            new MiniClusterResourceConfiguration.Builder()
                    .setNumberSlotsPerTaskManager(2)
                    .setNumberTaskManagers(1)
                    .build());

    // Test sink for Position updates
    public static class TestPositionSink implements Sink<Position> {
        private static final Queue<Position> collectedPositions = new ConcurrentLinkedQueue<>();

        public static void clear() {
            collectedPositions.clear();
        }

        public static List<Position> getResults() {
            return new ArrayList<>(collectedPositions);
        }

        @Override
        public SinkWriter<Position> createWriter(InitContext context) {
            return new SinkWriter<Position>() {
                @Override
                public void write(Position element, Context context) {
                    collectedPositions.add(copyPosition(element));
                }

                @Override
                public void flush(boolean endOfInput) {
                }

                @Override
                public void close() {
                }
            };
        }

        private Position copyPosition(Position pos) {
            Position copy = new Position();
            copy.accountId = pos.accountId;
            copy.symbol = pos.symbol;
            copy.netQty = pos.netQty;
            copy.avgPrice = pos.avgPrice;
            copy.realizedPnl = pos.realizedPnl;
            copy.unrealizedPnl = pos.unrealizedPnl;
            return copy;
        }
    }

    // Test sink for Portfolio updates
    public static class TestPortfolioSink implements Sink<Portfolio> {
        private static final Queue<Portfolio> collectedPortfolios = new ConcurrentLinkedQueue<>();

        public static void clear() {
            collectedPortfolios.clear();
        }

        public static List<Portfolio> getResults() {
            return new ArrayList<>(collectedPortfolios);
        }

        @Override
        public SinkWriter<Portfolio> createWriter(InitContext context) {
            return new SinkWriter<Portfolio>() {
                @Override
                public void write(Portfolio element, Context context) {
                    collectedPortfolios.add(copyPortfolio(element));
                }

                @Override
                public void flush(boolean endOfInput) {
                }

                @Override
                public void close() {
                }
            };
        }

        private Portfolio copyPortfolio(Portfolio pf) {
            Portfolio copy = new Portfolio();
            copy.accountId = pf.accountId;
            copy.equity = pf.equity;
            copy.cashBalance = pf.cashBalance;
            copy.exposure = pf.exposure;
            copy.marginUsed = pf.marginUsed;
            return copy;
        }
    }

    // Test sink for Risk alerts
    public static class TestRiskAlertSink implements Sink<RiskAlert> {
        private static final Queue<RiskAlert> collectedAlerts = new ConcurrentLinkedQueue<>();

        public static void clear() {
            collectedAlerts.clear();
        }

        public static List<RiskAlert> getResults() {
            return new ArrayList<>(collectedAlerts);
        }

        @Override
        public SinkWriter<RiskAlert> createWriter(InitContext context) {
            return new SinkWriter<RiskAlert>() {
                @Override
                public void write(RiskAlert element, Context context) {
                    collectedAlerts.add(copyRiskAlert(element));
                }

                @Override
                public void flush(boolean endOfInput) {
                }

                @Override
                public void close() {
                }
            };
        }

        private RiskAlert copyRiskAlert(RiskAlert alert) {
            RiskAlert copy = new RiskAlert();
            copy.accountId = alert.accountId;
            copy.message = alert.message;
            return copy;
        }
    }

    // Helper methods for creating test data
    private static TradeSignal createTradeSignal(String accountId, String symbol, double qty,
            double price,
            long timestamp) {
        return new TradeSignal(accountId, symbol, qty, price, timestamp);
    }

    private static ExecReport createExecReport(String accountId, String orderId, String symbol,
            double fillQty, double fillPrice, String status, long timestamp) {
        return new ExecReport(accountId, orderId, symbol, fillQty, fillPrice, status, timestamp);
    }

    private static DataStream<TradeSignal> createTradeSignalStream(StreamExecutionEnvironment env,
            List<TradeSignal> data) {
        DataStream<TradeSignal> base;
        if (data == null || data.isEmpty()) {
            base = env.fromElements(new TradeSignal("__empty__", "__empty__", 0.0, 0.0, 0L))
                    .filter(e -> false);
        } else {
            base = env.fromCollection(data);
        }
        return base.assignTimestampsAndWatermarks(
                WatermarkStrategy.<TradeSignal>forBoundedOutOfOrderness(
                        Duration.ofSeconds(1))
                        .withTimestampAssigner((e, ts) -> e.ts));
    }

    private static DataStream<ExecReport> createExecReportStream(StreamExecutionEnvironment env,
            List<ExecReport> data) {
        DataStream<ExecReport> base;
        if (data == null || data.isEmpty()) {
            base = env.fromElements(
                    createExecReport("__empty__", "__empty__", "__empty__", 0.0, 0.0, "FILLED", 0L))
                    .filter(e -> false);
        } else {
            base = env.fromCollection(data);
        }
        return base.assignTimestampsAndWatermarks(
                WatermarkStrategy.<ExecReport>forBoundedOutOfOrderness(
                        Duration.ofSeconds(1))
                        .withTimestampAssigner((e, ts) -> e.ts));
    }

    @Test
    public void testPositionUpdateFromExecutionReports() throws Exception {
        // Clear all test sinks
        TestPositionSink.clear();
        TestPortfolioSink.clear();
        TestRiskAlertSink.clear();

        // Create environment
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Create test data for position updates
        var baseTime = System.currentTimeMillis();

        var tradeSignals = Collections.<TradeSignal>emptyList();

        var execReports = Arrays.asList(
                // First buy order
                createExecReport("ACC001", "ORD001", "BTCUSD", 1.0, 50000.0, "FILLED", baseTime + 1000),
                // Second buy order at different price
                createExecReport("ACC001", "ORD002", "BTCUSD", 0.5, 52000.0, "FILLED", baseTime + 2000),
                // Partial sell
                createExecReport("ACC001", "ORD003", "BTCUSD", -0.3, 51000.0, "FILLED", baseTime + 3000));

        // Create streams
        var tradeSignalStream = createTradeSignalStream(env, tradeSignals);
        var execReportStream = createExecReportStream(env, execReports);

        // Create custom job with test sinks
        var job = new PortfolioAndRiskJobWithTestSinks(tradeSignalStream, execReportStream);
        job.run();

        // Execute
        env.execute("Portfolio Position Update Test");

        // Validate position updates
        var positions = TestPositionSink.getResults();
        assertFalse(positions.isEmpty(), "Expected position updates");

        // Find final position for BTCUSD
        var finalPosition = positions.stream()
                .filter(p -> "ACC001".equals(p.accountId) && "BTCUSD".equals(p.symbol))
                .reduce((first, second) -> second) // Get last position update
                .orElse(null);

        assertNotNull(finalPosition, "Expected final position for ACC001/BTCUSD");
        assertEquals("ACC001", finalPosition.accountId);
        assertEquals("BTCUSD", finalPosition.symbol);
        assertEquals(1.2, finalPosition.netQty, 0.001, "Expected net quantity of 1.2 BTC");

        // Average price should be positive and reflect weighted fills
        assertTrue(finalPosition.avgPrice > 0, "Average price should be positive");

        System.out.println("Position Update Test Results: " + positions);
    }

    @Test
    public void testOpenTradesLimitEnforced() throws Exception {
        TestPositionSink.clear();
        TestPortfolioSink.clear();
        TestRiskAlertSink.clear();

        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        var baseTime = System.currentTimeMillis();

        // Four buy signals on four distinct symbols for the same account
        var tradeSignals = Arrays.asList(
                createTradeSignal("ACC_LIMIT", "SYM1", 1.0, 1000.0, baseTime),
                createTradeSignal("ACC_LIMIT", "SYM2", 1.0, 1000.0, baseTime + 1),
                createTradeSignal("ACC_LIMIT", "SYM3", 1.0, 1000.0, baseTime + 2),
                createTradeSignal("ACC_LIMIT", "SYM4", 1.0, 1000.0, baseTime + 3));

        var execReports = Collections.<ExecReport>emptyList();

        var tradeSignalStream = createTradeSignalStream(env, tradeSignals);
        var execReportStream = createExecReportStream(env, execReports);

        var job = new PortfolioAndRiskJobWithTestSinks(tradeSignalStream, execReportStream);
        job.run();

        env.execute("Open Trades Limit Test");

        var positions = TestPositionSink.getResults();
        // Count distinct symbols that made it through as positions for ACC_LIMIT
        Set<String> symbols = new HashSet<>();
        for (Position p : positions) {
            if ("ACC_LIMIT".equals(p.accountId)) {
                symbols.add(p.symbol);
            }
        }
        // Expect at most 3 distinct symbols open (4th should be rejected)
        // Allow <= 3 to be robust to ordering; but we expect exactly 3 here
        org.junit.jupiter.api.Assertions.assertEquals(3, symbols.size(), "Should cap at 3 open symbols");
    }

    @Test
    public void testPortfolioAggregation() throws Exception {
        // Clear all test sinks
        TestPositionSink.clear();
        TestPortfolioSink.clear();
        TestRiskAlertSink.clear();

        // Create environment
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        var baseTime = System.currentTimeMillis();

        var tradeSignals = Arrays.asList(
                createTradeSignal("ACC002", "BTCUSD", 2.0, 50000.0, baseTime));
        // createTradeSignal("ACC002", "ETHUSD", 10.0, 3000.0, baseTime + 1000));

        var execReports = Arrays.asList(
                // BTC position
                createExecReport("ACC002", "ORD001", "BTCUSD", 2.0, 50000.0, "FILLED", baseTime + 1000),
                // ETH position
                createExecReport("ACC002", "ORD002", "ETHUSD", 10.0, 3000.0, "FILLED", baseTime + 2000));

        // Create streams
        var tradeSignalStream = createTradeSignalStream(env, tradeSignals);
        var execReportStream = createExecReportStream(env, execReports);

        // Create job
        var job = new PortfolioAndRiskJobWithTestSinks(tradeSignalStream, execReportStream);
        job.run();

        // Execute
        env.execute("Portfolio Aggregation Test");

        // Validate portfolio updates
        var portfolios = TestPortfolioSink.getResults();
        assertFalse(portfolios.isEmpty(), "Expected portfolio updates");

        // Check that portfolio includes both positions
        var finalPortfolio = portfolios.stream()
                .filter(p -> "ACC002".equals(p.accountId))
                .reduce((first, second) -> second) // Get last update
                .orElse(null);

        assertNotNull(finalPortfolio, "Expected final portfolio for ACC002");
        assertEquals("ACC002", finalPortfolio.accountId);
        assertTrue(finalPortfolio.exposure > 100000, "Expected significant exposure from BTC+ETH positions");

        System.out.println("Portfolio Aggregation Test Results: " + portfolios);
    }

    @Test
    public void testRiskAlertGeneration() throws Exception {
        // Clear all test sinks
        TestPositionSink.clear();
        TestPortfolioSink.clear();
        TestRiskAlertSink.clear();

        // Create environment
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        var baseTime = System.currentTimeMillis();

        var tradeSignals = Arrays.asList(
                createTradeSignal("ACC003", "BTCUSD", 25.0, 50000.0, baseTime) // Large position to trigger risk
        );

        var execReports = Arrays.asList(
                // Large BTC position that should trigger risk alert (25 BTC * 50k = 1.25M
                // exposure)
                createExecReport("ACC003", "ORD001", "BTCUSD", 25.0, 50000.0, "FILLED", baseTime + 1000));

        // Create streams
        var tradeSignalStream = createTradeSignalStream(env, tradeSignals);
        var execReportStream = createExecReportStream(env, execReports);

        // Create job
        var job = new PortfolioAndRiskJobWithTestSinks(tradeSignalStream, execReportStream);
        job.run();

        // Execute
        env.execute("Risk Alert Test");

        // Validate risk alerts
        var riskAlerts = TestRiskAlertSink.getResults();
        assertFalse(riskAlerts.isEmpty(), "Expected risk alerts for large exposure");

        var alert = riskAlerts.stream()
                .filter(a -> "ACC003".equals(a.accountId))
                .findFirst()
                .orElse(null);

        assertNotNull(alert, "Expected risk alert for ACC003");
        assertEquals("ACC003", alert.accountId);
        assertTrue(alert.message.contains("Exposure limit breached"), "Expected exposure limit breach message");

        System.out.println("Risk Alert Test Results: " + riskAlerts);
    }

    @Test
    public void testMultipleAccountsIsolation() throws Exception {
        // Clear all test sinks
        TestPositionSink.clear();
        TestPortfolioSink.clear();
        TestRiskAlertSink.clear();

        // Create environment
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        var baseTime = System.currentTimeMillis();

        var tradeSignals = Collections.<TradeSignal>emptyList();

        var execReports = Arrays.asList(
                createExecReport("ACC_A", "ORD001", "BTCUSD", 1.0, 50000.0, "FILLED", baseTime + 1000),
                createExecReport("ACC_B", "ORD002", "BTCUSD", 2.0, 50000.0, "FILLED", baseTime + 1500));

        // Create streams
        var tradeSignalStream = createTradeSignalStream(env, tradeSignals);
        var execReportStream = createExecReportStream(env, execReports);

        // Create job
        var job = new PortfolioAndRiskJobWithTestSinks(tradeSignalStream, execReportStream);
        job.run();

        // Execute
        env.execute("Multiple Accounts Isolation Test");

        // Validate account isolation
        var positions = TestPositionSink.getResults();
        var portfolios = TestPortfolioSink.getResults();

        // Check ACC_A position
        var accAPosition = positions.stream()
                .filter(p -> "ACC_A".equals(p.accountId) && "BTCUSD".equals(p.symbol))
                .findFirst()
                .orElse(null);
        assertNotNull(accAPosition, "Expected position for ACC_A");
        assertEquals(1.0, accAPosition.netQty, 0.001, "ACC_A should have 1.0 BTC");

        // Check ACC_B position
        var accBPosition = positions.stream()
                .filter(p -> "ACC_B".equals(p.accountId) && "BTCUSD".equals(p.symbol))
                .findFirst()
                .orElse(null);
        assertNotNull(accBPosition, "Expected position for ACC_B");
        assertEquals(2.0, accBPosition.netQty, 0.001, "ACC_B should have 2.0 BTC");

        // Verify portfolios are separate
        long accAPortfolioCount = portfolios.stream()
                .filter(pf -> "ACC_A".equals(pf.accountId))
                .count();
        long accBPortfolioCount = portfolios.stream()
                .filter(pf -> "ACC_B".equals(pf.accountId))
                .count();

        assertTrue(accAPortfolioCount > 0, "Expected portfolio updates for ACC_A");
        assertTrue(accBPortfolioCount > 0, "Expected portfolio updates for ACC_B");

        System.out.println(
                "Account Isolation Test - Positions: " + positions.size() + ", Portfolios: " + portfolios.size());
    }

    // Custom job class that uses test sinks instead of print statements
    static class PortfolioAndRiskJobWithTestSinks {
        private final DataStream<TradeSignal> tradeSignals;
        private final DataStream<ExecReport> execReports;

        public PortfolioAndRiskJobWithTestSinks(DataStream<TradeSignal> tradeSignals,
                DataStream<ExecReport> execReports) {
            this.tradeSignals = tradeSignals;
            this.execReports = execReports;
        }

        public void run() throws Exception {
            // === PRE-TRADE RISK CHECK ===
            DataStream<AccountPolicy> policyStream = tradeSignals
                    .map(ts -> new AccountPolicy(ts.accountId, 3, "ACTIVE", ts.ts))
                    .returns(AccountPolicy.class);

            DataStream<TradeSignal> acceptedOrders = tradeSignals
                    .keyBy(ts -> ts.accountId)
                    .connect(policyStream.keyBy(p -> p.accountId))
                    .process(new PreTradeRiskCheckWithPolicy());

            // Simulate execution for accepted orders
            DataStream<ExecReport> simulatedExecReports = acceptedOrders
                    .map(new FakeFill());

            // Combine external exec reports with simulated ones
            DataStream<ExecReport> allExecReports = execReports.union(simulatedExecReports);

            // === POSITION UPDATER ===
            DataStream<Position> positions = allExecReports
                    .keyBy(r -> r.accountId + "|" + r.symbol)
                    .process(new PositionUpdater());

            // === PORTFOLIO UPDATER ===
            DataStream<Portfolio> portfolios = positions
                    .keyBy(p -> p.accountId)
                    .process(new PortfolioUpdater());

            // === RISK ENGINE ===
            DataStream<RiskAlert> riskAlerts = portfolios
                    .process(new RiskEngine());

            // Use test sinks instead of print statements
            positions.sinkTo(new TestPositionSink());
            portfolios.sinkTo(new TestPortfolioSink());
            riskAlerts.sinkTo(new TestRiskAlertSink());
        }
    }
}
