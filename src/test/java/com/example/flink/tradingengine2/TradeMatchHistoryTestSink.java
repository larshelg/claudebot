package com.example.flink.tradingengine2;


import com.example.flink.tradingengine2.PositionUpdater;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class TradeMatchHistoryTestSink implements Sink<PositionUpdater.TradeMatch> {

    private static final List<PositionUpdater.TradeMatch> history = new CopyOnWriteArrayList<>();

    public static void clear() { history.clear(); }
    public static List<PositionUpdater.TradeMatch> all() { return new ArrayList<>(history); }

    @Override
    public SinkWriter<PositionUpdater.TradeMatch> createWriter(InitContext context) {
        return new SinkWriter<>() {
            @Override public void write(PositionUpdater.TradeMatch value, Context context) {
                history.add(copy(value));
            }
            @Override public void flush(boolean endOfInput) {}
            @Override public void close() {}
        };
    }

    private static PositionUpdater.TradeMatch copy(PositionUpdater.TradeMatch m) {
        PositionUpdater.TradeMatch x = new PositionUpdater.TradeMatch();
        x.matchId = m.matchId;
        x.accountId = m.accountId; x.strategyId = m.strategyId; x.symbol = m.symbol;
        x.buyOrderId = m.buyOrderId; x.buyFillId = m.buyFillId; x.buyTs = m.buyTs; x.buyPrice = m.buyPrice;
        x.sellOrderId = m.sellOrderId; x.sellFillId = m.sellFillId; x.sellTs = m.sellTs; x.sellPrice = m.sellPrice;
        x.matchedQty = m.matchedQty; x.realizedPnl = m.realizedPnl; x.matchTs = m.matchTs;
        return x;
    }
}
