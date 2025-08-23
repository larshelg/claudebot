package com.example.flink.tradeengine;

import com.example.flink.domain.AccountPolicy;
import com.example.flink.domain.TradeSignal;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.junit.ClassRule;
import org.junit.jupiter.api.Test;

import java.util.*;

import static com.example.flink.tradeengine.TestFlinkSinks.*;
import static com.example.flink.tradeengine.TestStreamHelpers.*;
import static org.junit.jupiter.api.Assertions.*;

public class PositionAndCloseIntegrationTest {

    @ClassRule
    static final MiniClusterWithClientResource flinkCluster = new MiniClusterWithClientResource(
            new MiniClusterResourceConfiguration.Builder()
                    .setNumberSlotsPerTaskManager(2)
                    .setNumberTaskManagers(1)
                    .build());

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

        var accountPolicyStream = tradeSignalStream
                .map(ts -> new AccountPolicy(ts.accountId, 3, "ACTIVE", 100_000.0, ts.ts))
                .returns(AccountPolicy.class);

        var job = new PortfolioAndRiskJob(tradeSignalStream, execReportStream, accountPolicyStream);
        job.setPositionSink(new TestUpsertSinks.PositionLatestSink());
        job.setPortfolioSink(new TestUpsertSinks.PortfolioLatestSink());
        job.setRiskAlertSink(new TestRiskAlertSink());
        job.setPositionCloseSink(new TestUpsertSinks.PositionCloseHistorySink());

        job.run();

        env.execute("Position Close Side Output Test");

        assertNull(TestUpsertSinks.PositionLatestSink.get("ACC_CLOSE", "XYZ"), "Position should be removed on close");

        var closes = TestUpsertSinks.PositionCloseHistorySink.getResults();
        assertFalse(closes.isEmpty(), "Expected a position close history row");
        var close = closes.stream()
                .filter(pc -> "ACC_CLOSE".equals(pc.accountId) && "XYZ".equals(pc.symbol))
                .findFirst().orElse(null);
        assertNotNull(close, "Expected close event for ACC_CLOSE/XYZ");
    }
}
