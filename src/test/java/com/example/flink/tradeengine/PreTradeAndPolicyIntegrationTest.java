package com.example.flink.tradeengine;

import com.example.flink.domain.AccountPolicy;
import com.example.flink.domain.ExecReport;
import com.example.flink.domain.Position;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.junit.ClassRule;
import org.junit.jupiter.api.Test;

import java.util.*;

import static com.example.flink.tradeengine.TestFlinkSinks.*;
import static com.example.flink.tradeengine.TestStreamHelpers.*;
import static org.junit.jupiter.api.Assertions.*;

public class PreTradeAndPolicyIntegrationTest {

    @ClassRule
    static final MiniClusterWithClientResource flinkCluster = new MiniClusterWithClientResource(
            new MiniClusterResourceConfiguration.Builder()
                    .setNumberSlotsPerTaskManager(2)
                    .setNumberTaskManagers(1)
                    .build());

    @Test
    public void testOpenTradesLimitEnforced() throws Exception {
        TestPositionSink.clear();
        TestPortfolioSink.clear();
        TestRiskAlertSink.clear();

        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        var baseTime = System.currentTimeMillis();

        var tradeSignals = Arrays.asList(
                createTradeSignal("ACC_LIMIT", "SYM1", 1.0, 1000.0, baseTime),
                createTradeSignal("ACC_LIMIT", "SYM2", 1.0, 1000.0, baseTime + 1),
                createTradeSignal("ACC_LIMIT", "SYM3", 1.0, 1000.0, baseTime + 2),
                createTradeSignal("ACC_LIMIT", "SYM4", 1.0, 1000.0, baseTime + 3));

        var execReports = Collections.<ExecReport>emptyList();

        var tradeSignalStream = createTradeSignalStream(env, tradeSignals);
        var execReportStream = createExecReportStream(env, execReports);

        var accountPolicyStream = tradeSignalStream
                .map(ts -> new AccountPolicy(ts.accountId, 3, "ACTIVE", 100_000.0, ts.ts))
                .returns(AccountPolicy.class);

        var job = new PortfolioAndRiskJob(tradeSignalStream, execReportStream, accountPolicyStream);
        job.setPositionSink(new TestPositionSink());
        job.setPortfolioSink(new TestPortfolioSink());
        job.setRiskAlertSink(new TestRiskAlertSink());
        job.run();

        env.execute("Open Trades Limit Test");

        var positions = TestPositionSink.getResults();
        Set<String> symbols = new HashSet<>();
        for (Position p : positions) {
            if ("ACC_LIMIT".equals(p.accountId)) {
                symbols.add(p.symbol);
            }
        }
        assertEquals(3, symbols.size(), "Should cap at 3 open symbols");
    }

    @Test
    public void testDynamicCapitalUpdates() throws Exception {
        TestPositionSink.clear();
        TestPortfolioSink.clear();
        TestRiskAlertSink.clear();

        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        var baseTime = System.currentTimeMillis();

        var tradeSignals = Arrays.asList(
                createTradeSignal("ACC_CAPITAL", "BTCUSD", 1.0, 50000.0, baseTime + 2000),
                createTradeSignal("ACC_CAPITAL", "ETHUSD", 10.0, 3000.0, baseTime + 4000));

        var accountPolicies = Arrays.asList(
                new AccountPolicy("ACC_CAPITAL", 3, "ACTIVE", 100_000.0, baseTime + 1000),
                new AccountPolicy("ACC_CAPITAL", 3, "ACTIVE", 50_000.0, baseTime + 3000));

        var execReports = Collections.<ExecReport>emptyList();

        var tradeSignalStream = createTradeSignalStreamWithWatermarks(env, tradeSignals);
        var accountPolicyStream = createAccountPolicyStreamWithWatermarks(env, accountPolicies);
        var execReportStream = createExecReportStream(env, execReports);

        var job = new PortfolioAndRiskJob(tradeSignalStream, execReportStream, accountPolicyStream);
        job.setPositionSink(new TestPositionSink());
        job.setPortfolioSink(new TestPortfolioSink());
        job.setRiskAlertSink(new TestRiskAlertSink());
        job.run();

        env.execute("Dynamic Capital Updates Test");

        var portfolios = TestPortfolioSink.getResults();
        assertFalse(portfolios.isEmpty(), "Expected portfolio updates");

        var accPortfolios = portfolios.stream()
                .filter(pf -> "ACC_CAPITAL".equals(pf.accountId))
                .toList();

        assertTrue(accPortfolios.size() >= 3, "Expected portfolio updates for capital changes and trades");
    }
}
