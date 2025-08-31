package com.example.flink.monitoring;


import com.example.flink.domain.*;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.Objects;

/**
 * Keyed by symbol. Keeps:
 *  - ValueState<PriceTick> latest price per symbol
 *  - MapState<(accountId,strategyId), RollupRow> all open positions for this symbol
 *
 * Emits UnrealizedPnlChange UPSERT on price/rollup updates, and DELETE when a rollup retracts or netQty ~ 0.
 */
public class SymbolJoinUnrealizedFn extends KeyedCoProcessFunction<String, RollupChange, PriceTick, UnrealizedPnlChange> {

    private transient ValueState<PriceTick> priceState;
    private transient MapState<AccountStrategyKey, RollupRow> positionsState;

    private static final double EPS = 1e-12;

    @Override
    public void open(Configuration parameters) {
        // Price per symbol
        ValueStateDescriptor<PriceTick> priceDesc = new ValueStateDescriptor<>("price", PriceTick.class);
        priceState = getRuntimeContext().getState(priceDesc);

        // All rollups per symbol
        MapStateDescriptor<AccountStrategyKey, RollupRow> posDesc =
                new MapStateDescriptor<>("positions", AccountStrategyKey.class, RollupRow.class);

        // Optional TTL so stale entries vanish if deletes are missed (belt-and-suspenders)
        StateTtlConfig ttl = StateTtlConfig.newBuilder(Time.days(3))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupInRocksdbCompactFilter(1000)
                .build();
        posDesc.enableTimeToLive(ttl);

        positionsState = getRuntimeContext().getMapState(posDesc);
    }

    // Stream 1: Rollup changelog
    @Override
    public void processElement1(RollupChange r, Context ctx, Collector<UnrealizedPnlChange> out) throws Exception {
        AccountStrategyKey k = new AccountStrategyKey(r.accountId, r.strategyId);
        PriceTick px = priceState.value();

        if (r.op == Op.DELETE || Math.abs(r.netQty) <= EPS) {
            // Remove and emit DELETE (always emit delete even if price unknown)
            positionsState.remove(k);
            out.collect(UnrealizedPnlChange.delete(r.accountId, r.strategyId, r.symbol, r.ts));
            return;
        }

        // UPSERT: store/refresh rollup row
        positionsState.put(k, new RollupRow(r.accountId, r.strategyId, r.symbol, r.netQty, r.avgPrice, r.ts));

        // If we have a price, compute and emit UPSERT
        if (px != null) {
            double pnl = (px.price - r.avgPrice) * r.netQty; // sign-safe: netQty negative for shorts
            out.collect(UnrealizedPnlChange.upsert(r.accountId, r.strategyId, r.symbol,
                    pnl, px.price, r.avgPrice, r.netQty, Math.max(px.ts, r.ts)));
        }
    }

    // Stream 2: Prices
    @Override
    public void processElement2(PriceTick p, Context ctx, Collector<UnrealizedPnlChange> out) throws Exception {
        priceState.update(p);

        // Recompute for all open positions of this symbol
        for (AccountStrategyKey k : positionsState.keys()) {
            RollupRow row = positionsState.get(k);
            if (row == null) continue;
            if (Math.abs(row.netQty) <= EPS) {
                // Defensive: purge and delete
                positionsState.remove(k);
                out.collect(UnrealizedPnlChange.delete(row.accountId, row.strategyId, row.symbol, p.ts));
                continue;
            }
            double pnl = (p.price - row.avgPrice) * row.netQty;
            out.collect(UnrealizedPnlChange.upsert(row.accountId, row.strategyId, row.symbol,
                    pnl, p.price, row.avgPrice, row.netQty, Math.max(p.ts, row.ts)));
        }
    }
}
