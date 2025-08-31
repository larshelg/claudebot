package com.example.flink.tradingengine2;

import com.example.flink.tradingengine2.TradingEngine;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

public class PositionUpdaterMiniClusterTest {

        @ClassRule
        static final MiniClusterWithClientResource flinkCluster = new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                        .setNumberSlotsPerTaskManager(2)
                                        .setNumberTaskManagers(1)
                                        .build());

        @AfterEach
        void tearDown() {
                OpenPositionsLotsTestSink.clear();
                OpenPositionsRollupTestSink.clear();
        }

        @Test
        void partialClose_stateIsCorrect() throws Exception {
                final String A = "acctA", S = "stratX", SYM = "BTCUSDT";
                long t1 = 1_000L, t2 = 2_000L, t3 = 3_000L;

                // BUY 5 @100, BUY 3 @105, SELL 6 @110 -> remain LONG 2 from 105-lot
                PositionUpdater.ExecReport e1 = new PositionUpdater.ExecReport(A, S, "o1", "f1", SYM, "BUY", 5, 100,
                                t1);
                PositionUpdater.ExecReport e2 = new PositionUpdater.ExecReport(A, S, "o2", "f1", SYM, "BUY", 3, 105,
                                t2);
                PositionUpdater.ExecReport e3 = new PositionUpdater.ExecReport(A, S, "o3", "f1", SYM, "SELL", 6, 110,
                                t3);

                // env + source (engine will assign its own watermark too; this is fine)
                StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                env.setParallelism(1);

                DataStream<PositionUpdater.ExecReport> src = env.fromElements(e1, e2, e3)
                                .assignTimestampsAndWatermarks(
                                                WatermarkStrategy.<PositionUpdater.ExecReport>forBoundedOutOfOrderness(
                                                                Duration.ofSeconds(1))
                                                                .withTimestampAssigner((er, ts) -> er.ts));

                // Call your TradingEngine.build with the two sinks
                TradingEngine.build(
                                env,
                                src,
                                new OpenPositionsLotsTestSink(),
                                new OpenPositionsRollupTestSink());

                env.execute("TradingEngine partial-close test");
                System.out.println("[Lots Ops]    " + OpenPositionsLotsTestSink.getOpEvents());
                System.out.println("[Lots Latest] " + OpenPositionsLotsTestSink.getAll());
                System.out.println("[Roll Ops]    " + OpenPositionsRollupTestSink.getOpEvents());
                System.out.println("[Roll Latest] " + OpenPositionsRollupTestSink.getAll());

                // === Assert intermediate state ===
                // Lots: o1#f1 should be deleted; o2#f1 should remain with qtyRem=2 @105
                PositionUpdater.PositionLot remaining = OpenPositionsLotsTestSink.get(A, S, SYM, "o2#f1");
                assertNotNull(remaining, "Expected remaining lot o2#f1");
                assertEquals(2.0, remaining.qtyRem, 1e-9);
                assertEquals("LONG", remaining.side);
                assertEquals(105.0, remaining.avgPrice, 1e-9);

                // Rollup: LONG 2 @105
                PositionUpdater.PositionRollup roll = OpenPositionsRollupTestSink.get(A, S, SYM);
                assertNotNull(roll, "Expected rollup row present");
                assertEquals(2.0, roll.netQty, 1e-9);
                assertEquals("LONG", roll.side);
                assertEquals(105.0, roll.avgPrice, 1e-9);

                // And we saw a delete for o1#f1
                assertTrue(OpenPositionsLotsTestSink.wasDeleted(A, S, SYM, "o1#f1"),
                                "Expected DELETE event for lot o1#f1");
        }

        @Test
        void fullCycle_flatEmitsDeletes() throws Exception {
                final String A = "acctA", S = "stratX", SYM = "BTCUSDT";
                long t1 = 1_000L, t2 = 2_000L, t3 = 3_000L, t4 = 4_000L, t5 = 5_000L;

                // BUY 5@100 -> BUY 3@105 -> SELL 6@110 -> SELL 5@95 -> BUY 3@90 (cover short)
                PositionUpdater.ExecReport e1 = new PositionUpdater.ExecReport(A, S, "o1", "f1", SYM, "BUY", 5, 100,
                                t1);
                PositionUpdater.ExecReport e2 = new PositionUpdater.ExecReport(A, S, "o2", "f1", SYM, "BUY", 3, 105,
                                t2);
                PositionUpdater.ExecReport e3 = new PositionUpdater.ExecReport(A, S, "o3", "f1", SYM, "SELL", 6, 110,
                                t3);
                PositionUpdater.ExecReport e4 = new PositionUpdater.ExecReport(A, S, "o4", "f1", SYM, "SELL", 5, 95,
                                t4);
                PositionUpdater.ExecReport e5 = new PositionUpdater.ExecReport(A, S, "o5", "f1", SYM, "BUY", 3, 90, t5);

                StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                env.setParallelism(1);

                DataStream<PositionUpdater.ExecReport> src = env.fromElements(e1, e2, e3, e4, e5)
                                .assignTimestampsAndWatermarks(
                                                WatermarkStrategy.<PositionUpdater.ExecReport>forBoundedOutOfOrderness(
                                                                Duration.ofSeconds(1))
                                                                .withTimestampAssigner((er, ts) -> er.ts));

                TradingEngine.build(
                                env,
                                src,
                                new OpenPositionsLotsTestSink(),
                                new OpenPositionsRollupTestSink());

                env.execute("TradingEngine full-cycle test");
                System.out.println("[Lots Ops]    " + OpenPositionsLotsTestSink.getOpEvents());
                System.out.println("[Lots Latest] " + OpenPositionsLotsTestSink.getAll());
                System.out.println("[Roll Ops]    " + OpenPositionsRollupTestSink.getOpEvents());
                System.out.println("[Roll Latest] " + OpenPositionsRollupTestSink.getAll());

                // Final snapshots: flat => both tables empty
                assertEquals(0, OpenPositionsLotsTestSink.size(), "All lots should be closed (no rows remain).");
                assertEquals(0, OpenPositionsRollupTestSink.size(), "Rollup should be deleted when flat.");

                // Op log: deletes for the expected lots and rollup
                assertTrue(OpenPositionsLotsTestSink.wasDeleted(A, S, SYM, "o1#f1"), "Expected DELETE for lot o1#f1");
                assertTrue(OpenPositionsLotsTestSink.wasDeleted(A, S, SYM, "o2#f1"), "Expected DELETE for lot o2#f1");
                assertTrue(OpenPositionsLotsTestSink.wasDeleted(A, S, SYM, "o4#f1"), "Expected DELETE for lot o4#f1");
                assertTrue(OpenPositionsRollupTestSink.wasDeleted(A, S, SYM), "Expected DELETE for rollup");
        }

        @Test
        void twoSymbols_mixedOutcomes() throws Exception {
                final String A = "acctA", S = "stratX";
                final String SYM1 = "BTCUSDT", SYM2 = "ETHUSDT";
                long t1 = 1_000L, t2 = 2_000L, t3 = 3_000L, t4 = 4_000L, t5 = 5_000L;
                // Symbol 1 (BTCUSDT): buy then fully close -> flat
                PositionUpdater.ExecReport s1_e1 = new PositionUpdater.ExecReport(A, S, "o1", "f1", SYM1, "BUY", 2, 100,
                                t1);
                PositionUpdater.ExecReport s1_e2 = new PositionUpdater.ExecReport(A, S, "o2", "f1", SYM1, "SELL", 2,
                                110, t2);
                // Symbol 2 (ETHUSDT): buy twice, then sell only the first lot -> remain LONG 2
                // @60
                PositionUpdater.ExecReport s2_e1 = new PositionUpdater.ExecReport(A, S, "o3", "f1", SYM2, "BUY", 3, 50,
                                t3);
                PositionUpdater.ExecReport s2_e2 = new PositionUpdater.ExecReport(A, S, "o4", "f1", SYM2, "BUY", 2, 60,
                                t4);
                PositionUpdater.ExecReport s2_e3 = new PositionUpdater.ExecReport(A, S, "o5", "f1", SYM2, "SELL", 3, 55,
                                t5);
                StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                env.setParallelism(1);
                DataStream<PositionUpdater.ExecReport> src = env.fromElements(
                                s1_e1, s1_e2,
                                s2_e1, s2_e2, s2_e3)
                                .assignTimestampsAndWatermarks(
                                                WatermarkStrategy.<PositionUpdater.ExecReport>forBoundedOutOfOrderness(
                                                                Duration.ofSeconds(1))
                                                                .withTimestampAssigner((er, ts) -> er.ts));
                TradingEngine.build(
                                env,
                                src,
                                new OpenPositionsLotsTestSink(),
                                new OpenPositionsRollupTestSink());
                env.execute("TradingEngine two-symbols mixed outcomes");
                System.out.println("[Lots Ops]    " + OpenPositionsLotsTestSink.getOpEvents());
                System.out.println("[Lots Latest] " + OpenPositionsLotsTestSink.getAll());
                System.out.println("[Roll Ops]    " + OpenPositionsRollupTestSink.getOpEvents());
                System.out.println("[Roll Latest] " + OpenPositionsRollupTestSink.getAll());
                // Symbol 1: fully flat -> lot deleted and rollup deleted, no remaining latest
                assertTrue(OpenPositionsLotsTestSink.wasDeleted(A, S, SYM1, "o1#f1"),
                                "SYM1: expected DELETE for lot o1#f1");
                assertTrue(OpenPositionsRollupTestSink.wasDeleted(A, S, SYM1), "SYM1: expected DELETE for rollup");
                assertNull(OpenPositionsLotsTestSink.get(A, S, SYM1, "o1#f1"));
                // Symbol 2: remaining LONG 2 @60 from second lot o4#f1; first lot o3#f1 deleted
                assertTrue(OpenPositionsLotsTestSink.wasDeleted(A, S, SYM2, "o3#f1"),
                                "SYM2: expected DELETE for first lot o3#f1");
                PositionUpdater.PositionLot ethRemaining = OpenPositionsLotsTestSink.get(A, S, SYM2, "o4#f1");
                assertNotNull(ethRemaining, "SYM2: expected remaining lot o4#f1");
                assertEquals(2.0, ethRemaining.qtyRem, 1e-9);
                assertEquals("LONG", ethRemaining.side);
                assertEquals(60.0, ethRemaining.avgPrice, 1e-9);
                PositionUpdater.PositionRollup ethRoll = OpenPositionsRollupTestSink.get(A, S, SYM2);
                assertNotNull(ethRoll, "SYM2: expected rollup row present");
                assertEquals(2.0, ethRoll.netQty, 1e-9);
                assertEquals("LONG", ethRoll.side);
                assertEquals(60.0, ethRoll.avgPrice, 1e-9);
        }
}
