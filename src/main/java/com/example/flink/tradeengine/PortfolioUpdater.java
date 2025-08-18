package com.example.flink.tradeengine;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import com.example.flink.domain.Portfolio;
import com.example.flink.domain.Position;

public class PortfolioUpdater extends KeyedProcessFunction<String, Position, Portfolio> {
    private transient ValueState<Portfolio> portfolioState;
    private transient MapState<String, Double> symbolExposureState;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Portfolio> desc = new ValueStateDescriptor<>(
                "portfolioState", Portfolio.class);
        portfolioState = getRuntimeContext().getState(desc);

        MapStateDescriptor<String, Double> mapDesc = new MapStateDescriptor<>(
                "symbolExposureState", Types.STRING, Types.DOUBLE);
        symbolExposureState = getRuntimeContext().getMapState(mapDesc);
    }

    @Override
    public void processElement(Position pos, Context ctx, Collector<Portfolio> out) throws Exception {
        Portfolio pf = portfolioState.value();
        if (pf == null) {
            pf = new Portfolio(pos.accountId);
        }

        double symbolExposure = Math.abs(pos.netQty * pos.avgPrice);
        symbolExposureState.put(pos.symbol, symbolExposure);

        double totalExposure = 0.0;
        for (String symbolKey : symbolExposureState.keys()) {
            Double e = symbolExposureState.get(symbolKey);
            if (e != null) {
                totalExposure += e;
            }
        }
        pf.exposure = totalExposure;
        pf.equity = pf.cashBalance + pf.exposure;

        portfolioState.update(pf);
        out.collect(pf);
    }
}