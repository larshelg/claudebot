package com.example.flink.tradeengine;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import com.example.flink.domain.Position;
import com.example.flink.domain.PriceTick;
import com.example.flink.domain.UnrealizedPnl;

/**
 * Joins latest position with incoming price ticks per account-symbol to compute
 * unrealized P&L.
 */
public class UnrealizedPnlCalculator extends KeyedCoProcessFunction<String, Position, PriceTick, UnrealizedPnl> {

    private transient ValueState<Position> latestPositionState;
    private transient ValueState<PriceTick> latestPriceState;

    @Override
    public void open(Configuration parameters) {
        latestPositionState = getRuntimeContext()
                .getState(new ValueStateDescriptor<>("latestPosition", Position.class));
        latestPriceState = getRuntimeContext().getState(new ValueStateDescriptor<>("latestPrice", PriceTick.class));
    }

    @Override
    public void processElement1(Position pos, Context ctx, Collector<UnrealizedPnl> out) throws Exception {
        latestPositionState.update(pos);
        PriceTick lastPrice = latestPriceState.value();
        if (lastPrice != null) {
            emitUnrealized(pos, lastPrice, out);
        }
    }

    @Override
    public void processElement2(PriceTick price, Context ctx, Collector<UnrealizedPnl> out) throws Exception {
        latestPriceState.update(price);
        Position pos = latestPositionState.value();
        if (pos != null) {
            emitUnrealized(pos, price, out);
        }
    }

    private void emitUnrealized(Position pos, PriceTick price, Collector<UnrealizedPnl> out) {
        double unrealized = (price.price - pos.avgPrice) * pos.netQty;
        out.collect(new UnrealizedPnl(pos.accountId, pos.symbol, unrealized, price.price, pos.avgPrice, pos.netQty,
                Math.max(pos.lastUpdated, price.ts)));
    }
}
