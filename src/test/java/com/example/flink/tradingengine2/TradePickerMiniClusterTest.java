package com.example.flink.tradingengine2;

import com.example.flink.domain.TradeSignal;
import com.example.flink.tradingengine2.TradePicker.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.ClassRule;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

public class TradePickerMiniClusterTest {

    @ClassRule
    static final MiniClusterWithClientResource flinkCluster = new MiniClusterWithClientResource(
            new MiniClusterResourceConfiguration.Builder()
                    .setNumberSlotsPerTaskManager(2)
                    .setNumberTaskManagers(1)
                    .build());

    @BeforeEach
    void setup() {
        TradeSignalCollectSink.clear();
        TradePickerRollupTestSink.clear();
    }

    @Test
    void routerPicksLeastLoadedAccountNotHoldingSymbol() throws Exception {
        // --- env
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(0L);
        env.getConfig().enableObjectReuse();

        // --- inputs
        var p1 = new AccountPolicy("A1", 2, "ACTIVE", System.currentTimeMillis());
        var p2 = new AccountPolicy("A2", 2, "ACTIVE", System.currentTimeMillis());
        var a1Eth = RollupChange.upsert("A1", "SMA", "ETH", 1.0, 100.0, 10L);

        var sig = new StrategySignal();
        sig.runId = "RUN1"; sig.symbol = "BTC"; sig.timestamp = 1000L; sig.signal = "BUY";

        var policies = env.fromElements(p1, p2)
                .returns(TypeInformation.of(new TypeHint<AccountPolicy>() {}));

        var rollups = env.fromElements(a1Eth)
                .returns(TypeInformation.of(new TypeHint<RollupChange>() {}));

        // Delay strategy slightly to let broadcast populate (cheap & deterministic here)
        var strategySignals = env.fromElements(sig)
                .returns(TypeInformation.of(new TypeHint<StrategySignal>() {}))
                .map(s -> { try { Thread.sleep(100); } catch (InterruptedException ignored) {} return s; });

        // --- call TradePicker.build
        var routed = TradePicker.build(
                env,
                strategySignals,
                rollups,
                policies,
                /*defaultQty*/ 1.0,
                /*allowScaleIn*/ true
        );

        // also attach rollup test sink so we can assert we saw the delta
        rollups.sinkTo(new TradePickerRollupTestSink()).name("rollup-test-sink");
        routed.sinkTo(new TradeSignalCollectSink()).name("trade-collect");

        // --- run
        env.execute("TradePicker MiniCluster Test");

        // --- asserts
        assertEquals(1, TradePickerRollupTestSink.size(), "one rollup row expected");
        var row = TradePickerRollupTestSink.get("A1", "SMA", "ETH");
        assertNotNull(row);
        assertEquals("A1", row.accountId);
        assertEquals("ETH", row.symbol);
        assertEquals(1.0, row.netQty, 1e-9);

        var out = TradeSignalCollectSink.drain();
        assertEquals(1, out.size(), "one trade expected");
        var t = out.get(0);

        assertEquals("A2", t.accountId, "should pick least-loaded account not holding BTC");
        assertEquals("RUN1-1000", t.signalId);
        assertEquals("BTC", t.symbol);
        assertEquals("BUY", t.action);
        assertEquals(1.0, t.qty, 1e-9);
    }

    @Test
    void oneAccount_rollups_gateTrades_whenCapacityExceeded() throws Exception {
        // --- env
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(0L);
        env.getConfig().enableObjectReuse();

        // --- policy: one ACTIVE account, maxOpenSymbols=3
        var policyA1 = new AccountPolicy("A1", 3, "ACTIVE", System.currentTimeMillis());

        var policies = env.fromElements(policyA1)
                .returns(TypeInformation.of(new TypeHint<AccountPolicy>() {}));

        // --- staged rollups for A1:
        // ETH, SOL arrive immediately  -> openCount becomes 2
        // XRP arrives a bit later      -> openCount becomes 3 (at capacity)
        // ADA arrives even later       -> openCount becomes 4 (> capacity)
        var rETH = RollupChange.upsert("A1", "STRAT", "ETH", 1.0, 100.0, 10L);
        var rSOL = RollupChange.upsert("A1", "STRAT", "SOL", 1.0,  50.0, 11L);
        var rXRP = RollupChange.upsert("A1", "STRAT", "XRP", 1.0,   1.0, 12L);
        var rADA = RollupChange.upsert("A1", "STRAT", "ADA", 1.0,   5.0, 13L);

        var rollups = env.fromElements(rETH, rSOL, rXRP, rADA)
                .returns(TypeInformation.of(new TypeHint<RollupChange>() {}))
                .map(r -> {
                    // simple timing: delay later symbols so strategy #1 routes before capacity is hit
                    if ("XRP".equals(r.symbol)) { try { Thread.sleep(150); } catch (InterruptedException ignored) {} }
                    if ("ADA".equals(r.symbol)) { try { Thread.sleep(300); } catch (InterruptedException ignored) {} }
                    return r;
                });

        // --- strategy signals:
        // S1: BUY BTC (after small delay; should be allowed while openCount==2)
        // S2: BUY DOGE (after long delay; should be rejected while openCount>=4)
        var s1 = new StrategySignal(); s1.runId="RUN"; s1.symbol="BTC";  s1.timestamp=1_000L; s1.signal="BUY";
        var s2 = new StrategySignal(); s2.runId="RUN"; s2.symbol="DOGE"; s2.timestamp=2_000L; s2.signal="BUY";

        var strategySignals = env.fromElements(s1, s2)
                .returns(TypeInformation.of(new TypeHint<StrategySignal>() {}))
                .map(s -> {
                    if ("BTC".equals(s.symbol))  { try { Thread.sleep( 50); } catch (InterruptedException ignored) {} } // before XRP/ADA
                    if ("DOGE".equals(s.symbol)) { try { Thread.sleep(400); } catch (InterruptedException ignored) {} } // after ADA
                    return s;
                });

        // --- call TradePicker.build (broadcast router)
        var routed = TradePicker.build(
                env,
                strategySignals,
                rollups,
                policies,
                /*defaultQty*/ 1.0,
                /*allowScaleIn*/ true
        );

        // (optional) also check that rollups were seen
        rollups.sinkTo(new TradePickerRollupTestSink()).name("rollup-test-sink");

        // collect routed trades
        routed.sinkTo(new TradeSignalCollectSink()).name("trade-collect");

        // run
        env.execute("TradePicker single-account capacity test");

        // --- assert: we saw all rollups
        assertEquals(4, TradePickerRollupTestSink.size(), "expected 4 rollup rows recorded");

        // --- assert: only the first trade (BTC) was routed; DOGE was blocked at/above capacity
        var out = TradeSignalCollectSink.drain();
        assertEquals(1, out.size(), "only one trade should pass capacity gate");

        var t = out.get(0);
        assertEquals("A1", t.accountId);
        assertEquals("BTC", t.symbol);
        assertEquals("BUY", t.action);
        assertEquals("RUN-1000", t.signalId);
        assertEquals(1.0, t.qty, 1e-9);
    }
}

