package com.example.flink.tradeengine.fluss;

import com.alibaba.fluss.flink.source.deserializer.FlussDeserializationSchema;
import com.alibaba.fluss.record.LogRecord;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.types.RowType;
import com.example.flink.domain.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.table.data.*;

import java.util.function.Function;

/** Misc adapters for Fluss integration. */
public class Adapters {

    /** Deserializer for ExecReport from fluss.exec_report_history */
    public static class ExecReportDeserializationSchema implements FlussDeserializationSchema<ExecReport> {
        @Override
        public void open(InitializationContext context) {
        }

        @Override
        public ExecReport deserialize(LogRecord record) {
            InternalRow row = record.getRow();
            String accountId = row.getString(0).toString();
            String orderId = row.getString(1).toString();
            String symbol = row.getString(2).toString();
            double fillQty = row.getDouble(3);
            double fillPrice = row.getDouble(4);
            String status = row.getString(5).toString();
            long ts = row.getTimestampNtz(6, 3).getMillisecond();
            return new ExecReport(accountId, orderId, symbol, fillQty, fillPrice, status, ts);
        }

        @Override
        public TypeInformation<ExecReport> getProducedType(RowType rowType) {
            return TypeInformation.of(ExecReport.class);
        }
    }

    /** Deserializer for TradeSignal from fluss.trade_signal_history */
    public static class TradeSignalDeserializationSchema
            implements FlussDeserializationSchema<com.example.flink.domain.TradeSignal> {
        @Override
        public void open(InitializationContext context) {
        }

        @Override
        public com.example.flink.domain.TradeSignal deserialize(LogRecord record) {
            InternalRow row = record.getRow();
            String accountId = row.getString(0).toString();
            String symbol = row.getString(1).toString();
            double qty = row.getDouble(2);
            double price = row.getDouble(3);
            long ts = row.getTimestampNtz(4, 3).getMillisecond();
            return new com.example.flink.domain.TradeSignal(accountId, symbol, qty, price, ts);
        }

        @Override
        public TypeInformation<com.example.flink.domain.TradeSignal> getProducedType(RowType rowType) {
            return TypeInformation.of(com.example.flink.domain.TradeSignal.class);
        }
    }

    /** Deserializer for StrategySignal from fluss.strategy_signals */
    public static class StrategySignalDeserializationSchema
            implements FlussDeserializationSchema<StrategySignal> {
        @Override
        public void open(InitializationContext context) {
        }

        @Override
        public StrategySignal deserialize(LogRecord record) {
            InternalRow row = record.getRow();
            String runId = row.getString(0).toString();
            String symbol = row.getString(1).toString();
            long timestamp = row.getTimestampNtz(2, 3).getMillisecond();
            double close = row.getDecimal(3, 10, 4).toBigDecimal().doubleValue();
            double sma5 = row.getDecimal(4, 18, 8).toBigDecimal().doubleValue();
            double sma21 = row.getDecimal(5, 18, 8).toBigDecimal().doubleValue();
            String signal = row.getString(6).toString();
            double signalStrength = row.getDecimal(7, 5, 2).toBigDecimal().doubleValue();
            return new StrategySignal(runId, symbol, timestamp, close, sma5, sma21, signal,
                    signalStrength);
        }

        @Override
        public TypeInformation<StrategySignal> getProducedType(RowType rowType) {
            return TypeInformation.of(StrategySignal.class);
        }
    }

    /** Deserializer for Position from fluss.open_positions_latest */
    public static class PositionLatestDeserializationSchema
            implements FlussDeserializationSchema<com.example.flink.domain.Position> {
        @Override
        public void open(InitializationContext context) {
        }

        @Override
        public com.example.flink.domain.Position deserialize(LogRecord record) {
            InternalRow row = record.getRow();
            com.example.flink.domain.Position p = new com.example.flink.domain.Position();
            p.accountId = row.getString(0).toString();
            p.symbol = row.getString(1).toString();
            p.netQty = row.getDouble(2);
            p.avgPrice = row.getDouble(3);
            p.lastUpdated = row.getTimestampNtz(4, 3).getMillisecond();
            return p;
        }

        @Override
        public TypeInformation<com.example.flink.domain.Position> getProducedType(RowType rowType) {
            return TypeInformation.of(com.example.flink.domain.Position.class);
        }
    }

    /** Adapter sink: map POJO to RowData for Fluss sink. */
    public static class PojoToFlussSink<T> implements Sink<T> {
        private final Sink<RowData> delegate;
        private final Function<T, RowData> mapper;

        public PojoToFlussSink(Sink<RowData> delegate, Function<T, RowData> mapper) {
            this.delegate = delegate;
            this.mapper = mapper;
        }

        @Override
        public SinkWriter<T> createWriter(InitContext context) {
            final SinkWriter<RowData> rowWriter;
            try {
                rowWriter = delegate.createWriter(context);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return new SinkWriter<T>() {
                @Override
                public void write(T element, Context context) {
                    try {
                        rowWriter.write(mapper.apply(element), context);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }

                @Override
                public void flush(boolean endOfInput) {
                    try {
                        rowWriter.flush(endOfInput);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }

                @Override
                public void close() {
                    try {
                        rowWriter.close();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            };
        }
    }

    /** Row mappers for Fluss tables. */
    public static class FlussRowMappers {
        public static RowData tradeSignalToRow(com.example.flink.domain.TradeSignal t) {
            GenericRowData row = new GenericRowData(5);
            row.setField(0, StringData.fromString(t.accountId));
            row.setField(1, StringData.fromString(t.symbol));
            row.setField(2, t.qty);
            row.setField(3, t.price);
            row.setField(4, TimestampData.fromEpochMillis(t.ts));
            return row;
        }

        public static RowData positionToRow(Position p) {
            GenericRowData row = new GenericRowData(5);
            row.setField(0, StringData.fromString(p.accountId));
            row.setField(1, StringData.fromString(p.symbol));
            row.setField(2, p.netQty);
            row.setField(3, p.avgPrice);
            row.setField(4, TimestampData.fromEpochMillis(p.lastUpdated));
            return row;
        }

        public static RowData portfolioToRow(Portfolio pf) {
            GenericRowData row = new GenericRowData(5);
            row.setField(0, StringData.fromString(pf.accountId));
            row.setField(1, pf.equity);
            row.setField(2, pf.cashBalance);
            row.setField(3, pf.exposure);
            row.setField(4, pf.marginUsed);
            return row;
        }

        public static RowData realizedToRow(RealizedPnl r) {
            GenericRowData row = new GenericRowData(4);
            row.setField(0, StringData.fromString(r.accountId));
            row.setField(1, StringData.fromString(r.symbol));
            row.setField(2, r.realizedPnl);
            row.setField(3, TimestampData.fromEpochMillis(r.ts));
            return row;
        }

        public static RowData unrealizedToRow(UnrealizedPnl u) {
            GenericRowData row = new GenericRowData(7);
            row.setField(0, StringData.fromString(u.accountId));
            row.setField(1, StringData.fromString(u.symbol));
            row.setField(2, u.unrealizedPnl);
            row.setField(3, u.currentPrice);
            row.setField(4, u.avgPrice);
            row.setField(5, u.netQty);
            row.setField(6, TimestampData.fromEpochMillis(u.ts));
            return row;
        }

        public static RowData execReportToRow(ExecReport er) {
            GenericRowData row = new GenericRowData(7);
            row.setField(0, StringData.fromString(er.accountId));
            row.setField(1, StringData.fromString(er.orderId));
            row.setField(2, StringData.fromString(er.symbol));
            row.setField(3, er.fillQty);
            row.setField(4, er.fillPrice);
            row.setField(5, StringData.fromString(er.status));
            row.setField(6, TimestampData.fromEpochMillis(er.ts));
            return row;
        }

        public static RowData tradeMatchToRow(TradeMatch m) {
            GenericRowData row = new GenericRowData(12);
            row.setField(0, StringData.fromString(m.matchId));
            row.setField(1, StringData.fromString(m.accountId));
            row.setField(2, StringData.fromString(m.symbol));
            row.setField(3, StringData.fromString(m.buyOrderId));
            row.setField(4, StringData.fromString(m.sellOrderId));
            row.setField(5, m.matchedQty);
            row.setField(6, m.buyPrice);
            row.setField(7, m.sellPrice);
            row.setField(8, m.realizedPnl);
            row.setField(9, TimestampData.fromEpochMillis(m.matchTimestamp));
            row.setField(10, TimestampData.fromEpochMillis(m.buyTimestamp));
            row.setField(11, TimestampData.fromEpochMillis(m.sellTimestamp));
            return row;
        }

        public static RowData positionCloseToRow(PositionClose pc) {
            GenericRowData row = new GenericRowData(7);
            row.setField(0, StringData.fromString(pc.accountId));
            row.setField(1, StringData.fromString(pc.symbol));
            row.setField(2, pc.totalQty);
            row.setField(3, pc.avgPrice);
            row.setField(4, TimestampData.fromEpochMillis(pc.openTs));
            row.setField(5, TimestampData.fromEpochMillis(pc.closeTs));
            row.setField(6, pc.realizedPnl);
            return row;
        }

        public static RowData riskAlertToRow(RiskAlert ra) {
            GenericRowData row = new GenericRowData(2);
            row.setField(0, StringData.fromString(ra.accountId));
            row.setField(1, StringData.fromString(ra.message));
            return row;
        }
    }
}
