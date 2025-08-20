package com.example.flink.tradeengine;

import com.example.flink.domain.AccountPolicy;
import com.example.flink.domain.ExecReport;
import com.example.flink.domain.Portfolio;
import com.example.flink.domain.Position;
import com.example.flink.domain.RiskAlert;
import com.example.flink.domain.TradeMatch;
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
            copy.lastUpdated = pos.lastUpdated;
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

    // Test sink for ExecReport tracking
    public static class TestExecReportSink implements Sink<ExecReport> {
        private static final Queue<ExecReport> collectedExecReports = new ConcurrentLinkedQueue<>();

        public static void clear() {
            collectedExecReports.clear();
        }

        public static List<ExecReport> getResults() {
            return new ArrayList<>(collectedExecReports);
        }

        @Override
        public SinkWriter<ExecReport> createWriter(InitContext context) {
            return new SinkWriter<ExecReport>() {
                @Override
                public void write(ExecReport element, Context context) {
                    collectedExecReports.add(copyExecReport(element));
                }

                @Override
                public void flush(boolean endOfInput) {
                }

                @Override
                public void close() {
                }
            };
        }

        private ExecReport copyExecReport(ExecReport exec) {
            return new ExecReport(exec.accountId, exec.orderId, exec.symbol,
                    exec.fillQty, exec.fillPrice, exec.status, exec.ts);
        }
    }

    // Test sink for TradeMatch tracking
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

    // Test implementation of TrackingSinkFactory
    public static class TestTrackingSinkFactory implements TrackingSinkFactory {
        @Override
        public Sink<ExecReport> createExecReportSink() {
            return new TestExecReportSink();
        }

        @Override
        public Sink<TradeMatch> createTradeMatchSink() {
            return new TestTradeMatchSink();
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

        // Create job with test sinks
        var job = new PortfolioAndRiskJob(tradeSignalStream, execReportStream,
                new TestPositionSink(), new TestPortfolioSink(), new TestRiskAlertSink());
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

        var job = new PortfolioAndRiskJob(tradeSignalStream, execReportStream,
                new TestPositionSink(), new TestPortfolioSink(), new TestRiskAlertSink());
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
        var job = new PortfolioAndRiskJob(tradeSignalStream, execReportStream,
                new TestPositionSink(), new TestPortfolioSink(), new TestRiskAlertSink());
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
        var job = new PortfolioAndRiskJob(tradeSignalStream, execReportStream,
                new TestPositionSink(), new TestPortfolioSink(), new TestRiskAlertSink());
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
        var job = new PortfolioAndRiskJob(tradeSignalStream, execReportStream,
                new TestPositionSink(), new TestPortfolioSink(), new TestRiskAlertSink());
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

    @Test
    public void testDynamicCapitalUpdates() throws Exception {
        // Clear all test sinks
        TestPositionSink.clear();
        TestPortfolioSink.clear();
        TestRiskAlertSink.clear();

        // Create environment
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        var baseTime = System.currentTimeMillis();

        // Create trade signals with controlled timing
        var tradeSignals = Arrays.asList(
                // First trade after initial capital
                createTradeSignal("ACC_CAPITAL", "BTCUSD", 1.0, 50000.0, baseTime + 2000),
                // Second trade after capital update
                createTradeSignal("ACC_CAPITAL", "ETHUSD", 10.0, 3000.0, baseTime + 4000));

        // Create account policies with capital updates
        var accountPolicies = Arrays.asList(
                // Initial capital
                new AccountPolicy("ACC_CAPITAL", 3, "ACTIVE", 100_000.0, baseTime + 1000),
                // Capital withdrawal (simulate withdrawal to $50k)
                new AccountPolicy("ACC_CAPITAL", 3, "ACTIVE", 50_000.0, baseTime + 3000));

        var execReports = Collections.<ExecReport>emptyList();

        // Create streams with proper watermarks for event time processing
        var tradeSignalStream = createTradeSignalStreamWithWatermarks(env, tradeSignals);
        var accountPolicyStream = createAccountPolicyStreamWithWatermarks(env, accountPolicies);
        var execReportStream = createExecReportStream(env, execReports);

        // Create job with custom policy stream and test sinks
        var job = new PortfolioAndRiskJob(tradeSignalStream, execReportStream, accountPolicyStream,
                new TestPositionSink(), new TestPortfolioSink(), new TestRiskAlertSink());
        job.run();

        // Execute
        env.execute("Dynamic Capital Updates Test");

        // Validate portfolio updates show capital changes
        var portfolios = TestPortfolioSink.getResults();
        assertFalse(portfolios.isEmpty(), "Expected portfolio updates");

        // Find portfolios for ACC_CAPITAL
        var accPortfolios = portfolios.stream()
                .filter(pf -> "ACC_CAPITAL".equals(pf.accountId))
                .toList();

        assertTrue(accPortfolios.size() >= 4, "Expected at least 4 portfolio updates");

        // Find specific portfolio states
        var initialCapitalPortfolio = accPortfolios.stream()
                .filter(pf -> pf.cashBalance == 100_000.0 && pf.exposure == 0.0)
                .findFirst().orElse(null);

        var capitalWithdrawalPortfolio = accPortfolios.stream()
                .filter(pf -> pf.cashBalance == 50_000.0 && pf.exposure == 0.0)
                .findFirst().orElse(null);

        var firstTradePortfolio = accPortfolios.stream()
                .filter(pf -> pf.cashBalance == 50_000.0 && pf.exposure == 50_000.0)
                .findFirst().orElse(null);

        var secondTradePortfolio = accPortfolios.stream()
                .filter(pf -> pf.cashBalance == 50_000.0 && pf.exposure == 80_000.0)
                .findFirst().orElse(null);

        // Verify initial capital setting
        assertNotNull(initialCapitalPortfolio, "Should have initial capital portfolio");
        assertEquals(100_000.0, initialCapitalPortfolio.cashBalance, 0.01);
        assertEquals(0.0, initialCapitalPortfolio.exposure, 0.01);

        // Verify capital withdrawal
        assertNotNull(capitalWithdrawalPortfolio, "Should have capital withdrawal portfolio");
        assertEquals(50_000.0, capitalWithdrawalPortfolio.cashBalance, 0.01);
        assertEquals(0.0, capitalWithdrawalPortfolio.exposure, 0.01);

        // Verify first trade uses updated capital
        assertNotNull(firstTradePortfolio, "Should have first trade portfolio");
        assertEquals(50_000.0, firstTradePortfolio.cashBalance, 0.01);
        assertEquals(50_000.0, firstTradePortfolio.exposure, 0.01);

        // Verify second trade builds on updated capital
        assertNotNull(secondTradePortfolio, "Should have second trade portfolio");
        assertEquals(50_000.0, secondTradePortfolio.cashBalance, 0.01);
        assertEquals(80_000.0, secondTradePortfolio.exposure, 0.01);
        assertEquals(130_000.0, secondTradePortfolio.equity, 0.01);

        // Debug output removed to test for flakiness
        // System.out.println("Dynamic Capital Test Results:");
        // System.out.println("Portfolio updates: " + accPortfolios.size());
        // System.out.println("All portfolios:");
        // accPortfolios.forEach(pf -> System.out.printf(
        // "Cash: $%.0f, Exposure: $%.0f, Equity: $%.0f%n",
        // pf.cashBalance, pf.exposure, pf.equity));

        // Also check positions
        var positions = TestPositionSink.getResults();
        var accPositions = positions.stream()
                .filter(pos -> "ACC_CAPITAL".equals(pos.accountId))
                .toList();
        // System.out.println("Positions created: " + accPositions.size());
        // accPositions.forEach(pos -> System.out.printf(
        // "Symbol: %s, Qty: %.2f, Price: %.2f%n",
        // pos.symbol, pos.netQty, pos.avgPrice));
    }

    // Helper method to create AccountPolicy stream with watermarks
    private static DataStream<AccountPolicy> createAccountPolicyStreamWithWatermarks(
            StreamExecutionEnvironment env, List<AccountPolicy> data) {
        DataStream<AccountPolicy> base;
        if (data == null || data.isEmpty()) {
            base = env.fromElements(new AccountPolicy("__empty__", 0, "ACTIVE", 0.0, 0L))
                    .filter(e -> false);
        } else {
            base = env.fromCollection(data);
        }
        return base.assignTimestampsAndWatermarks(
                WatermarkStrategy.<AccountPolicy>forBoundedOutOfOrderness(
                        Duration.ofSeconds(1))
                        .withTimestampAssigner((policy, ts) -> policy.ts));
    }

    // Helper method to create TradeSignal stream with watermarks
    private static DataStream<TradeSignal> createTradeSignalStreamWithWatermarks(
            StreamExecutionEnvironment env, List<TradeSignal> data) {
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
                        .withTimestampAssigner((signal, ts) -> signal.ts));
    }

    @Test
    public void testExecReportTracking() throws Exception {
        // Clear all test sinks
        TestPositionSink.clear();
        TestPortfolioSink.clear();
        TestRiskAlertSink.clear();
        TestExecReportSink.clear();

        // Create environment
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        var baseTime = System.currentTimeMillis();

        // Create trade signals that will generate simulated exec reports
        var tradeSignals = Arrays.asList(
                createTradeSignal("ACC_TRACK", "BTCUSD", 1.0, 50000.0, baseTime + 1000),
                createTradeSignal("ACC_TRACK", "ETHUSD", 5.0, 3000.0, baseTime + 2000));

        // Create real exec reports from broker
        var execReports = Arrays.asList(
                createExecReport("ACC_TRACK", "ORD001", "ADAUSD", 100.0, 1.5, "FILLED", baseTime + 3000),
                createExecReport("ACC_TRACK", "ORD002", "DOTUSD", 50.0, 25.0, "FILLED", baseTime + 4000));

        // Create streams
        var tradeSignalStream = createTradeSignalStream(env, tradeSignals);
        var execReportStream = createExecReportStream(env, execReports);

        // Create job with tracking enabled - using 3-stream constructor with tracking
        var accountPolicyStream = tradeSignalStream
                .map(ts -> new AccountPolicy(ts.accountId, 3, "ACTIVE", 100_000.0, ts.ts))
                .returns(AccountPolicy.class);

        var job = new PortfolioAndRiskJob(tradeSignalStream, execReportStream, accountPolicyStream,
                new TestPositionSink(), new TestPortfolioSink(), new TestRiskAlertSink(),
                new TestTrackingSinkFactory());
        job.run();

        // Execute
        env.execute("ExecReport Tracking Test");

        // Validate ExecReport tracking
        var trackedExecReports = TestExecReportSink.getResults();
        assertFalse(trackedExecReports.isEmpty(), "Expected tracked exec reports");

        // Should have both simulated exec reports (from trade signals) and real exec
        // reports
        var accTrackExecReports = trackedExecReports.stream()
                .filter(exec -> "ACC_TRACK".equals(exec.accountId))
                .toList();

        assertTrue(accTrackExecReports.size() >= 4, "Expected at least 4 exec reports (2 simulated + 2 real)");

        // Verify we have exec reports for all expected symbols
        var symbols = accTrackExecReports.stream()
                .map(exec -> exec.symbol)
                .collect(java.util.stream.Collectors.toSet());

        assertTrue(symbols.contains("BTCUSD"), "Should track BTCUSD exec report from trade signal");
        assertTrue(symbols.contains("ETHUSD"), "Should track ETHUSD exec report from trade signal");
        assertTrue(symbols.contains("ADAUSD"), "Should track ADAUSD exec report from broker");
        assertTrue(symbols.contains("DOTUSD"), "Should track DOTUSD exec report from broker");

        // Verify exec report details
        var btcExecReport = accTrackExecReports.stream()
                .filter(exec -> "BTCUSD".equals(exec.symbol))
                .findFirst().orElse(null);
        assertNotNull(btcExecReport, "Should have BTC exec report");
        assertEquals(1.0, btcExecReport.fillQty, 0.001, "BTC fill quantity should match trade signal");
        assertEquals(50000.0, btcExecReport.fillPrice, 0.01, "BTC fill price should match trade signal");
        assertEquals("FILLED", btcExecReport.status, "Status should be FILLED");

        var adaExecReport = accTrackExecReports.stream()
                .filter(exec -> "ADAUSD".equals(exec.symbol))
                .findFirst().orElse(null);
        assertNotNull(adaExecReport, "Should have ADA exec report");
        assertEquals(100.0, adaExecReport.fillQty, 0.001, "ADA fill quantity should match broker report");
        assertEquals(1.5, adaExecReport.fillPrice, 0.01, "ADA fill price should match broker report");
        assertEquals("FILLED", adaExecReport.status, "Status should be FILLED");

        System.out.println("ExecReport Tracking Test Results:");
        System.out.println("Total tracked exec reports: " + trackedExecReports.size());
        accTrackExecReports.forEach(exec -> System.out.printf(
                "Symbol: %s, OrderId: %s, Qty: %.2f, Price: %.2f, Status: %s%n",
                exec.symbol, exec.orderId, exec.fillQty, exec.fillPrice, exec.status));
    }

    @Test
    public void testUpsertLatestSinks() throws Exception {
        // Clear all sinks
        TestUpsertSinks.PositionLatestSink.clear();
        TestUpsertSinks.RealizedPnlLatestSink.clear();
        TestUpsertSinks.UnrealizedPnlLatestSink.clear();

        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        var baseTime = System.currentTimeMillis();

        // No signals needed; drive with exec reports
        var tradeSignals = Collections.<TradeSignal>emptyList();
        var execReports = Arrays.asList(
                // Buy 1 @ 100
                createExecReport("ACC_UPSERT", "B1", "XYZ", 1.0, 100.0, "FILLED", baseTime + 1000),
                // Sell 0.6 @ 110 (realized P&L = (110-100)*0.6 = 6.0)
                createExecReport("ACC_UPSERT", "S1", "XYZ", -0.6, 110.0, "FILLED", baseTime + 2000));

        var tradeSignalStream = createTradeSignalStream(env, tradeSignals);
        var execReportStream = createExecReportStream(env, execReports);

        // Use the extended constructor to wire latest-snapshot test sinks
        var job = new PortfolioAndRiskJob(
                tradeSignalStream,
                execReportStream,
                tradeSignalStream.map(ts -> new AccountPolicy(ts.accountId, 3, "ACTIVE", 100_000.0, ts.ts))
                        .returns(AccountPolicy.class),
                new TestUpsertSinks.PositionLatestSink(),
                new TestUpsertSinks.PortfolioLatestSink(),
                new TestRiskAlertSink(),
                null, // TrackingSinkFactory
                null, // TradeMatch sink (not needed here)
                new TestUpsertSinks.RealizedPnlLatestSink(),
                new TestUpsertSinks.UnrealizedPnlLatestSink(),
                new TestUpsertSinks.PositionCloseHistorySink());

        job.run();

        env.execute("Upsert Latest Sinks Test");

        // Validate realized PnL latest
        var realized = TestUpsertSinks.RealizedPnlLatestSink.get("ACC_UPSERT", "XYZ");
        assertNotNull(realized, "Expected realized P&L upsert row");
        assertEquals(6.0, realized.realizedPnl, 1e-6, "Expected realized P&L of 6.0");

        // Validate position latest reflects remaining 0.4
        var pos = TestUpsertSinks.PositionLatestSink.get("ACC_UPSERT", "XYZ");
        assertNotNull(pos, "Expected position upsert row");
        assertEquals(0.4, pos.netQty, 1e-6, "Expected remaining net qty of 0.4");

        // Validate unrealized PnL latest using last trade price (110) vs avg price ~100
        var unreal = TestUpsertSinks.UnrealizedPnlLatestSink.get("ACC_UPSERT", "XYZ");
        assertNotNull(unreal, "Expected unrealized P&L upsert row");
        // Unrealized = (110 - 100) * 0.4 = 4.0
        assertEquals(4.0, unreal.unrealizedPnl, 1e-6, "Expected unrealized P&L of 4.0");
    }

    @Test
    public void testPositionCloseSideOutputAndRemoval() throws Exception {
        TestUpsertSinks.PositionLatestSink.clear();
        TestUpsertSinks.PositionCloseHistorySink.clear();

        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        var baseTime = System.currentTimeMillis();

        var tradeSignals = Collections.<TradeSignal>emptyList();
        var execReports = Arrays.asList(
                createExecReport("ACC_CLOSE", "B1", "XYZ", 1.0, 100.0, "FILLED", baseTime + 1000),
                createExecReport("ACC_CLOSE", "S1", "XYZ", -1.0, 110.0, "FILLED", baseTime + 2000));

        var tradeSignalStream = createTradeSignalStream(env, tradeSignals);
        var execReportStream = createExecReportStream(env, execReports);

        var job = new PortfolioAndRiskJob(
                tradeSignalStream,
                execReportStream,
                tradeSignalStream.map(ts -> new AccountPolicy(ts.accountId, 3, "ACTIVE", 100_000.0, ts.ts))
                        .returns(AccountPolicy.class),
                new TestUpsertSinks.PositionLatestSink(),
                new TestUpsertSinks.PortfolioLatestSink(),
                new TestRiskAlertSink(),
                null, // tracking factory
                null, // tradeMatch
                null, // realized
                null, // unrealized
                new TestUpsertSinks.PositionCloseHistorySink());

        // We need to capture side output of position closes; the job prints them. For
        // testing,
        // we reuse the print, but we also rely on PositionLatestSink to remove the
        // entry when netQty=0
        job.run();

        env.execute("Position Close Side Output Test");

        // Validate position latest no longer contains the symbol
        assertNull(TestUpsertSinks.PositionLatestSink.get("ACC_CLOSE", "XYZ"), "Position should be removed on close");
    }

}
