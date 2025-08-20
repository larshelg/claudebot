package com.example.flink.tradeengine;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Collector;

import com.example.flink.domain.ExecReport;
import com.example.flink.domain.Position;
import com.example.flink.domain.PositionClose;

public class PositionUpdater extends KeyedProcessFunction<String, ExecReport, Position> {
    private transient ValueState<Position> state;
    public static final OutputTag<PositionClose> POSITION_CLOSE_TAG = new OutputTag<PositionClose>("position-close") {
    };

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
            double previousNet = pos.netQty;
            double fillQty = report.fillQty;
            double remaining = previousNet + fillQty;

            if (Math.abs(previousNet) == 0.0) {
                // Opening new position
                pos.avgPrice = report.fillPrice;
                pos.netQty = fillQty;
            } else if (Math.signum(previousNet) == Math.signum(fillQty)) {
                // Increasing existing position in same direction → weighted average
                double totalQty = previousNet + fillQty;
                pos.avgPrice = ((previousNet * pos.avgPrice) + (fillQty * report.fillPrice)) / totalQty;
                pos.netQty = totalQty;
            } else {
                // Reducing existing position
                if (Math.signum(remaining) == Math.signum(previousNet)) {
                    // Reduced but still same direction → avgPrice unchanged
                    pos.netQty = remaining;
                } else if (Math.abs(remaining) == 0.0) {
                    // Fully closed → net becomes zero, avg unchanged, handled below
                    pos.netQty = 0.0;
                } else {
                    // Crossed to opposite side → new position with remainder at current fill price
                    pos.avgPrice = report.fillPrice;
                    pos.netQty = remaining;
                }
            }
            pos.lastUpdated = report.ts; // Use execution timestamp
        }

        // Emit close event and clear state when position flattens
        if (Math.abs(pos.netQty) == 0.0) {
            // Emit a zero-qty position update to signal deletion to upsert-like sinks
            out.collect(pos);

            PositionClose close = new PositionClose(
                    pos.accountId,
                    pos.symbol,
                    0.0,
                    pos.avgPrice,
                    0L,
                    pos.lastUpdated,
                    0.0);
            ctx.output(POSITION_CLOSE_TAG, close);
            state.clear();
        } else {
            state.update(pos);
            out.collect(pos);
        }
    }
}
