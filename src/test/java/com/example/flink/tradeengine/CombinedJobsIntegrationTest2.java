package com.example.flink.tradeengine;

import com.example.flink.domain.StrategySignal;
import com.example.flink.domain.AccountPolicy;
import com.example.flink.domain.Position;
import com.example.flink.exchange.LocalTestOrderExecutionJob;
import com.example.flink.strategyengine.StrategyChooserJob;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class CombinedJobsIntegrationTest2 {

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
}
