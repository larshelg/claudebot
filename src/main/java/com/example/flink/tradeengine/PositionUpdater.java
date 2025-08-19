package com.example.flink.tradeengine;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import com.example.flink.domain.ExecReport;
import com.example.flink.domain.Position;

public class PositionUpdater extends KeyedProcessFunction<String, ExecReport, Position> {
    private transient ValueState<Position> state;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Position> desc = new ValueStateDescriptor<>(
                "positionState", Position.class);
        state = getRuntimeContext().getState(desc);
    }

    @Override
    public void processElement(ExecReport report, Context ctx, Collector<Position> out) throws Exception {
        Position pos = state.value();
        if (pos == null) {
            pos = new Position(report.accountId, report.symbol);
        }

        if ("FILLED".equals(report.status)) {
            double totalQty = pos.netQty + report.fillQty;
            if (totalQty != 0) {
                pos.avgPrice = ((pos.netQty * pos.avgPrice) + (report.fillQty * report.fillPrice)) / totalQty;
            }
            pos.netQty = totalQty;
            pos.lastUpdated = report.ts; // Use execution timestamp
        }

        state.update(pos);
        out.collect(pos);
    }
}