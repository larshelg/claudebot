package com.example.flink.tradeengine;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import com.example.flink.domain.RealizedPnl;
import com.example.flink.domain.TradeMatch;

/**
 * Aggregates realized P&L over time per account-symbol key based on TradeMatch
 * records.
 */
public class RealizedPnlAggregator extends KeyedProcessFunction<String, TradeMatch, RealizedPnl> {

    private transient ValueState<Double> cumulativeRealizedPnlState;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Double> desc = new ValueStateDescriptor<>("cumRealizedPnl", Double.class);
        cumulativeRealizedPnlState = getRuntimeContext().getState(desc);
    }

    @Override
    public void processElement(TradeMatch match, Context ctx, Collector<RealizedPnl> out) throws Exception {
        Double cum = cumulativeRealizedPnlState.value();
        if (cum == null)
            cum = 0.0;

        cum += match.realizedPnl;
        cumulativeRealizedPnlState.update(cum);

        // Emit updated realized PnL snapshot
        String[] parts = ctx.getCurrentKey().split("\\|");
        String accountId = parts.length > 0 ? parts[0] : match.accountId;
        String symbol = parts.length > 1 ? parts[1] : match.symbol;
        out.collect(new RealizedPnl(accountId, symbol, cum, match.matchTimestamp));
    }
}
