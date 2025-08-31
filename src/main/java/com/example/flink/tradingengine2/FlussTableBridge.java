package com.example.flink.tradingengine2;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.sql.Timestamp;

public final class FlussTableBridge {

    private FlussTableBridge() {
    }

    public static void attach(StreamTableEnvironment tEnv,
            DataStream<PositionUpdater.PositionLotChange> lots,
            DataStream<PositionUpdater.PositionRollupChange> rollup,
            DataStream<PositionUpdater.RealizedPnlChange> pnlLatest) {

        // LOTS → RowKind
        DataStream<Row> lotsRows = lots.map(ch -> {
            Row row = (ch.op == PositionUpdater.Op.DELETE)
                    ? Row.withNames(RowKind.DELETE)
                    : Row.withNames(RowKind.UPDATE_AFTER);
            row.setField("accountId", ch.key.accountId);
            row.setField("strategyId", ch.key.strategyId);
            row.setField("symbol", ch.key.symbol);
            row.setField("lotId", ch.key.lotId);
            if (ch.op == PositionUpdater.Op.UPSERT) {
                row.setField("side", ch.row.side);
                row.setField("qtyOpen", ch.row.qtyOpen);
                row.setField("qtyRem", ch.row.qtyRem);
                row.setField("avgPrice", ch.row.avgPrice);
                row.setField("tsOpen", new Timestamp(ch.row.tsOpen));
                row.setField("tsUpdated", new Timestamp(ch.ts));
            }
            return row;
        });

        Table lotsTable = tEnv.fromChangelogStream(
                lotsRows,
                Schema.newBuilder()
                        .column("accountId", "STRING")
                        .column("strategyId", "STRING")
                        .column("symbol", "STRING")
                        .column("lotId", "STRING")
                        .column("side", "STRING")
                        .column("qtyOpen", "DOUBLE")
                        .column("qtyRem", "DOUBLE")
                        .column("avgPrice", "DOUBLE")
                        .column("tsOpen", "TIMESTAMP(3)")
                        .column("tsUpdated", "TIMESTAMP(3)")
                        .primaryKey("accountId", "strategyId", "symbol", "lotId")
                        .build());
        tEnv.createTemporaryView("lots_updates_view", lotsTable);
        tEnv.executeSql("INSERT INTO fluss.open_positions_lots SELECT * FROM lots_updates_view");

        // ROLLUP
        DataStream<Row> rollRows = rollup.map(ch -> {
            Row row = (ch.op == PositionUpdater.Op.DELETE)
                    ? Row.withNames(RowKind.DELETE)
                    : Row.withNames(RowKind.UPDATE_AFTER);
            row.setField("accountId", ch.key.accountId);
            row.setField("strategyId", ch.key.strategyId);
            row.setField("symbol", ch.key.symbol);
            if (ch.op == PositionUpdater.Op.UPSERT) {
                row.setField("side", ch.row.side);
                row.setField("netQty", ch.row.netQty);
                row.setField("avgPrice", ch.row.avgPrice);
                row.setField("lastUpdated", new Timestamp(ch.ts));
            }
            return row;
        });

        Table rollTable = tEnv.fromChangelogStream(
                rollRows,
                Schema.newBuilder()
                        .column("accountId", "STRING")
                        .column("strategyId", "STRING")
                        .column("symbol", "STRING")
                        .column("side", "STRING")
                        .column("netQty", "DOUBLE")
                        .column("avgPrice", "DOUBLE")
                        .column("lastUpdated", "TIMESTAMP(3)")
                        .primaryKey("accountId", "strategyId", "symbol")
                        .build());
        tEnv.createTemporaryView("rollup_updates_view", rollTable);
        tEnv.executeSql("INSERT INTO fluss.open_positions_rollup SELECT * FROM rollup_updates_view");

        // REALIZED PNL LATEST (UPSERT only)
        DataStream<Row> pnlRows = pnlLatest.map(ch -> {
            Row row = Row.withNames(RowKind.UPDATE_AFTER);
            row.setField("accountId", ch.accountId);
            row.setField("strategyId", ch.strategyId);
            row.setField("symbol", ch.symbol);
            row.setField("realizedPnl", ch.realizedPnl);
            row.setField("ts", new Timestamp(ch.ts));
            return row;
        });

        Table pnlTable = tEnv.fromChangelogStream(
                pnlRows,
                Schema.newBuilder()
                        .column("accountId", "STRING")
                        .column("strategyId", "STRING")
                        .column("symbol", "STRING")
                        .column("realizedPnl", "DOUBLE")
                        .column("ts", "TIMESTAMP(3)")
                        .primaryKey("accountId", "strategyId", "symbol")
                        .build());
        tEnv.createTemporaryView("realized_pnl_latest_view", pnlTable);
        tEnv.executeSql("INSERT INTO fluss.realized_pnl_latest SELECT * FROM realized_pnl_latest_view");
    }
}
