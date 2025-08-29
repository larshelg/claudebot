package com.example.flink.tradingengine2;

// package com.example.flink.tradingengine2;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.sql.Timestamp;

public final class FlussMatchBridge {
    private FlussMatchBridge(){}

    public static void attach(StreamTableEnvironment tEnv,
                              DataStream<PositionUpdater.TradeMatch> matches) {

        DataStream<Row> rows = matches.map(m -> {
            Row r = Row.withNames(RowKind.INSERT);
            r.setField("matchId", m.matchId);
            r.setField("accountId", m.accountId);
            r.setField("strategyId", m.strategyId);
            r.setField("symbol", m.symbol);
            r.setField("buyOrderId", m.buyOrderId);
            r.setField("sellOrderId", m.sellOrderId);
            r.setField("matchedQty", m.matchedQty);
            r.setField("buyPrice", m.buyPrice);
            r.setField("sellPrice", m.sellPrice);
            r.setField("realizedPnl", m.realizedPnl);
            r.setField("buyTimestamp", new Timestamp(m.buyTs));
            r.setField("sellTimestamp", new Timestamp(m.sellTs));
            r.setField("matchTimestamp", new Timestamp(m.matchTs));
            return r;
        });

        Table t = tEnv.fromChangelogStream(
                rows,
                Schema.newBuilder()
                        .column("matchId", "STRING")
                        .column("accountId", "STRING")
                        .column("strategyId", "STRING")
                        .column("symbol", "STRING")
                        .column("buyOrderId", "STRING")
                        .column("sellOrderId", "STRING")
                        .column("matchedQty", "DOUBLE")
                        .column("buyPrice", "DOUBLE")
                        .column("sellPrice", "DOUBLE")
                        .column("realizedPnl", "DOUBLE")
                        .column("buyTimestamp", "TIMESTAMP(3)")
                        .column("sellTimestamp", "TIMESTAMP(3)")
                        .column("matchTimestamp", "TIMESTAMP(3)")
                        .build()
        );
        tEnv.createTemporaryView("trade_match_history_view", t);
        tEnv.executeSql("INSERT INTO fluss.trade_match_history SELECT * FROM trade_match_history_view");
    }
}
