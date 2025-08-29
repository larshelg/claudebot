package com.example.flink.tradingengine2;

// package com.example.flink.tradingengine2.test;

import com.example.flink.tradingengine2.PositionUpdater.ExecReport;
import com.example.flink.tradingengine2.PositionUpdater.TradeMatch;
import com.example.flink.tradingengine2.TradingEngine;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class TradeMatchHistoryStreamTest {

    @AfterEach
    void tearDown() {
        OpenPositionsLotsTestSink.clear();
        OpenPositionsRollupTestSink.clear();
        RealizedPnlLatestTestSink.clear();
        TradeMatchHistoryTestSink.clear();
    }

    @Test
    void matches_are_emitted_with_correct_legs_and_pnl() throws Exception {
        final String A="acctA", S="stratX", SYM="BTCUSDT";
        long t1=1_000L, t2=2_000L, t3=3_000L, t4=4_000L, t5=5_000L;

        // BUY 5@100 (o1) -> BUY 3@105 (o2)
        // SELL 6@110 (o3) => match 5@100 (+50), 1@105 (+5)
        // SELL 5@95  (o4) => match 2@105 (-20), open SHORT 3@95
        // BUY  3@90  (o5) => cover 3@95 (+15)
        ExecReport e1 = new ExecReport(A,S,"o1","f1",SYM,"BUY",  5,100,t1);
        ExecReport e2 = new ExecReport(A,S,"o2","f1",SYM,"BUY",  3,105,t2);
        ExecReport e3 = new ExecReport(A,S,"o3","f1",SYM,"SELL", 6,110,t3);
        ExecReport e4 = new ExecReport(A,S,"o4","f1",SYM,"SELL", 5, 95,t4);
        ExecReport e5 = new ExecReport(A,S,"o5","f1",SYM,"BUY",  3, 90,t5);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<ExecReport> src = env.fromElements(e1,e2,e3,e4,e5)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<ExecReport>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                                .withTimestampAssigner((er, ts) -> er.ts)
                );

        TradingEngine.build(
                env,
                src,
                new OpenPositionsLotsTestSink(),
                new OpenPositionsRollupTestSink(),
                new RealizedPnlLatestTestSink(),
                new TradeMatchHistoryTestSink()
        );

        env.execute("TradeMatchHistory stream test");

        List<TradeMatch> ms = TradeMatchHistoryTestSink.all();
        assertEquals(4, ms.size(), "Expected 4 match events (5,1,2,3).");

        // helper lambdas
        double sumQty = ms.stream().mapToDouble(m -> m.matchedQty).sum();
        double sumPnl = ms.stream().mapToDouble(m -> m.realizedPnl).sum();

        assertEquals(11.0, sumQty, 1e-9);     // 5 + 1 + 2 + 3
        assertEquals(50.0, sumPnl, 1e-9);     // 50 + 5 - 20 + 15

        // Check first two matches from SELL 6@110 (o3)
        TradeMatch m1 = ms.get(0);
        assertEquals("o1", m1.buyOrderId);  assertEquals("o3", m1.sellOrderId);
        assertEquals(5.0, m1.matchedQty, 1e-9);
        assertEquals(100.0, m1.buyPrice, 1e-9);  assertEquals(110.0, m1.sellPrice, 1e-9);
        assertEquals(50.0, m1.realizedPnl, 1e-9);

        TradeMatch m2 = ms.get(1);
        assertEquals("o2", m2.buyOrderId);  assertEquals("o3", m2.sellOrderId);
        assertEquals(1.0, m2.matchedQty, 1e-9);
        assertEquals(105.0, m2.buyPrice, 1e-9);  assertEquals(110.0, m2.sellPrice, 1e-9);
        assertEquals(5.0, m2.realizedPnl, 1e-9);

        // Third match from SELL 5@95 (o4) consuming remaining long 2 from o2
        TradeMatch m3 = ms.get(2);
        assertEquals("o2", m3.buyOrderId);  assertEquals("o4", m3.sellOrderId);
        assertEquals(2.0, m3.matchedQty, 1e-9);
        assertEquals(105.0, m3.buyPrice, 1e-9);  assertEquals(95.0, m3.sellPrice, 1e-9);
        assertEquals(-20.0, m3.realizedPnl, 1e-9);

        // Fourth match covering SHORT 3 (o4 entry) with BUY 3@90 (o5)
        TradeMatch m4 = ms.get(3);
        assertEquals("o5", m4.buyOrderId);  assertEquals("o4", m4.sellOrderId);
        assertEquals(3.0, m4.matchedQty, 1e-9);
        assertEquals(90.0, m4.buyPrice, 1e-9);   assertEquals(95.0, m4.sellPrice, 1e-9);
        assertEquals(15.0, m4.realizedPnl, 1e-9);
    }
}
