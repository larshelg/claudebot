package com.example.flink.tradingengine2;

import com.example.flink.tradingengine2.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

public class RealizedPnlStreamTest {

        @AfterEach
        void tearDown() {
                OpenPositionsLotsTestSink.clear();
                OpenPositionsRollupTestSink.clear();
                RealizedPnlLatestTestSink.clear();
        }

        @Test
        void partialClose_realizedIs55() throws Exception {
                final String A = "acctA", S = "stratX", SYM = "BTCUSDT";
                long t1 = 1_000L, t2 = 2_000L, t3 = 3_000L;

                PositionUpdater.ExecReport e1 = new PositionUpdater.ExecReport(A, S, "o1", "f1", SYM, "BUY", 5, 100,
                                t1);
                PositionUpdater.ExecReport e2 = new PositionUpdater.ExecReport(A, S, "o2", "f1", SYM, "BUY", 3, 105,
                                t2);
                PositionUpdater.ExecReport e3 = new PositionUpdater.ExecReport(A, S, "o3", "f1", SYM, "SELL", 6, 110,
                                t3); // +55

                StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                env.setParallelism(1);

                DataStream<PositionUpdater.ExecReport> src = env.fromElements(e1, e2, e3)
                                .assignTimestampsAndWatermarks(
                                                WatermarkStrategy.<PositionUpdater.ExecReport>forBoundedOutOfOrderness(
                                                                Duration.ofSeconds(1))
                                                                .withTimestampAssigner((er, ts) -> er.ts));

                TradingEngine.build(env, src,
                                new OpenPositionsLotsTestSink(),
                                new OpenPositionsRollupTestSink(),
                                new RealizedPnlLatestTestSink());

                env.execute("RealizedPnL partial close");

                var r = RealizedPnlLatestTestSink.get(A, S, SYM);
                assertNotNull(r, "Expected realizedPnl latest present");
                assertEquals(55.0, r.realizedPnl, 1e-9);
        }

        @Test
        void fullCycle_realizedEndsAt50() throws Exception {
                final String A = "acctA", S = "stratX", SYM = "BTCUSDT";
                long t1 = 1_000L, t2 = 2_000L, t3 = 3_000L, t4 = 4_000L, t5 = 5_000L;

                PositionUpdater.ExecReport e1 = new PositionUpdater.ExecReport(A, S, "o1", "f1", SYM, "BUY", 5, 100,
                                t1);
                PositionUpdater.ExecReport e2 = new PositionUpdater.ExecReport(A, S, "o2", "f1", SYM, "BUY", 3, 105,
                                t2);
                PositionUpdater.ExecReport e3 = new PositionUpdater.ExecReport(A, S, "o3", "f1", SYM, "SELL", 6, 110,
                                t3); // +55
                PositionUpdater.ExecReport e4 = new PositionUpdater.ExecReport(A, S, "o4", "f1", SYM, "SELL", 5, 95,
                                t4); // -20 on remaining long 2; + SHORT 3 opens
                PositionUpdater.ExecReport e5 = new PositionUpdater.ExecReport(A, S, "o5", "f1", SYM, "BUY", 3, 90, t5); // +15
                                                                                                                         // covering
                                                                                                                         // short
                                                                                                                         // 3
                // total = 55 - 20 + 15 = 50

                StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                env.setParallelism(1);

                DataStream<PositionUpdater.ExecReport> src = env.fromElements(e1, e2, e3, e4, e5)
                                .assignTimestampsAndWatermarks(
                                                WatermarkStrategy.<PositionUpdater.ExecReport>forBoundedOutOfOrderness(
                                                                Duration.ofSeconds(1))
                                                                .withTimestampAssigner((er, ts) -> er.ts));

                TradingEngine.build(env, src,
                                new OpenPositionsLotsTestSink(),
                                new OpenPositionsRollupTestSink(),
                                new RealizedPnlLatestTestSink());

                env.execute("RealizedPnL full cycle");

                var r = RealizedPnlLatestTestSink.get(A, S, SYM);
                assertNotNull(r, "Expected realizedPnl latest present");
                assertEquals(50.0, r.realizedPnl, 1e-9);
        }
}
