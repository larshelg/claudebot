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

public class TradeMatchingIntegrationTest {

    @ClassRule
    static final MiniClusterWithClientResource flinkCluster = new MiniClusterWithClientResource(
            new MiniClusterResourceConfiguration.Builder()
                    .setNumberSlotsPerTaskManager(2)
                    .setNumberTaskManagers(1)
                    .build());

    @Test
    public void testTradeMatchHistoryContents() throws Exception {
        TestTradeMatchSink.clear();

        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        var baseTime = System.currentTimeMillis();

        var tradeSignals = Collections.<TradeSignal>emptyList();
        var execReports = Arrays.asList(
                createExecReport("ACC_MATCH", "B1", "XYZ", 1.0, 100.0, "FILLED", baseTime + 1000),
                createExecReport("ACC_MATCH", "S1", "XYZ", -0.6, 110.0, "FILLED", baseTime + 2000));

        var tradeSignalStream = createTradeSignalStream(env, tradeSignals);
        var execReportStream = createExecReportStream(env, execReports);

        var accountPolicyStream = tradeSignalStream
                .map(ts -> new AccountPolicy(ts.accountId, 3, "ACTIVE", 100_000.0, ts.ts))
                .returns(AccountPolicy.class);

        var job = new PortfolioAndRiskJob(tradeSignalStream, execReportStream, accountPolicyStream);
        job.setTradeMatchSink(new TestTradeMatchSink());

        job.run();

        env.execute("TradeMatch History Test");

        var matches = TestTradeMatchSink.getResults();
        assertFalse(matches.isEmpty(), "Expected trade match history rows");
        var match = matches.stream()
                .filter(m -> "ACC_MATCH".equals(m.accountId) && "XYZ".equals(m.symbol))
                .findFirst().orElse(null);
        assertNotNull(match, "Expected trade match for ACC_MATCH/XYZ");
        assertEquals(0.6, match.matchedQty, 1e-6, "Matched qty should be 0.6");
        assertEquals(100.0, match.buyPrice, 1e-6, "Buy price should be 100");
        assertEquals(110.0, match.sellPrice, 1e-6, "Sell price should be 110");
        assertEquals(6.0, match.realizedPnl, 1e-6, "Expected realized P&L of 6.0");
    }
}
