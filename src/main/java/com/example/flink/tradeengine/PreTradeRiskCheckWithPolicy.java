package com.example.flink.tradeengine;

import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import com.example.flink.domain.AccountPolicy;
import com.example.flink.domain.TradeSignal;

public class PreTradeRiskCheckWithPolicy
        extends KeyedCoProcessFunction<String, TradeSignal, AccountPolicy, TradeSignal> {
    private static final double MAX_ORDER_NOTIONAL = 1_000_000.0;
    private static final int DEFAULT_MAX_OPEN_SYMBOLS = 3;

    private transient MapState<String, Double> estimatedNetQtyBySymbol;
    private transient ValueState<Integer> openSymbolsCount;
    private transient ValueState<Integer> maxOpenSymbolsState;

    @Override
    public void open(Configuration parameters) {
        MapStateDescriptor<String, Double> netDesc = new MapStateDescriptor<>(
                "estimatedNetQtyBySymbol", Types.STRING, Types.DOUBLE);
        estimatedNetQtyBySymbol = getRuntimeContext().getMapState(netDesc);

        ValueStateDescriptor<Integer> countDesc = new ValueStateDescriptor<>(
                "openSymbolsCount", Integer.class);
        openSymbolsCount = getRuntimeContext().getState(countDesc);

        ValueStateDescriptor<Integer> maxOpenDesc = new ValueStateDescriptor<>(
                "maxOpenSymbols", Integer.class);
        maxOpenSymbolsState = getRuntimeContext().getState(maxOpenDesc);
    }

    @Override
    public void processElement1(TradeSignal signal, Context ctx, Collector<TradeSignal> out) throws Exception {
        double notional = Math.abs(signal.qty * signal.price);
        if (notional > MAX_ORDER_NOTIONAL) {
            return;
        }

        Integer count = openSymbolsCount.value();
        if (count == null)
            count = 0;

        Integer maxOpen = maxOpenSymbolsState.value();
        if (maxOpen == null)
            maxOpen = DEFAULT_MAX_OPEN_SYMBOLS;

        Double currentNet = estimatedNetQtyBySymbol.get(signal.symbol);
        if (currentNet == null)
            currentNet = 0.0;

        boolean currentlyClosed = Math.abs(currentNet) == 0.0;
        boolean wouldOpen = currentlyClosed && Math.abs(signal.qty) > 0.0;

        if (wouldOpen && count >= maxOpen) {
            return;
        }

        double updatedNet = currentNet + signal.qty;
        if (Math.abs(updatedNet) == 0.0) {
            estimatedNetQtyBySymbol.remove(signal.symbol);
            if (!currentlyClosed) {
                if (count > 0)
                    openSymbolsCount.update(count - 1);
                else
                    openSymbolsCount.update(0);
            }
        } else {
            estimatedNetQtyBySymbol.put(signal.symbol, updatedNet);
            if (currentlyClosed) {
                openSymbolsCount.update(count + 1);
            }
        }

        out.collect(signal);
    }

    @Override
    public void processElement2(AccountPolicy policy, Context ctx, Collector<TradeSignal> out) throws Exception {
        if (policy == null || policy.accountId == null)
            return;
        if ("BLOCKED".equalsIgnoreCase(policy.status)) {
            maxOpenSymbolsState.update(0);
        } else if (policy.maxOpenSymbols > 0) {
            maxOpenSymbolsState.update(policy.maxOpenSymbols);
        } else {
            maxOpenSymbolsState.update(DEFAULT_MAX_OPEN_SYMBOLS);
        }
    }
}