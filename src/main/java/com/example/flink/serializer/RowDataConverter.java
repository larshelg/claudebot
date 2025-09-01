package com.example.flink.serializer;

import com.example.flink.domain.StrategySignal;
import org.apache.flink.table.data.*;

import java.math.BigDecimal;

public class RowDataConverter {

    public static RowData toRowData(StrategySignal signal) {
        GenericRowData row = new GenericRowData(8);

        row.setField(0, StringData.fromString("live"));
        row.setField(1, StringData.fromString(signal.symbol));
        row.setField(2, TimestampData.fromEpochMillis(signal.timestamp));
        row.setField(3, DecimalData.fromBigDecimal(BigDecimal.valueOf(signal.close), 10, 4));
        row.setField(4, DecimalData.fromBigDecimal(BigDecimal.valueOf(signal.sma5), 18, 8));
        row.setField(5, DecimalData.fromBigDecimal(BigDecimal.valueOf(signal.sma21), 18, 8));
        row.setField(6, StringData.fromString(signal.signal));
        row.setField(7, DecimalData.fromBigDecimal(BigDecimal.valueOf(signal.signalStrength), 5, 2));

        return row;
    }

    public static StrategySignal fromRowData(RowData row) {
        String runId = row.getString(0).toString();
        String symbol = row.getString(1).toString();
        long timestamp = row.getTimestamp(2, 3).getMillisecond();
        double close = row.getDecimal(3, 10, 4).toBigDecimal().doubleValue();
        double sma5 = row.getDecimal(4, 18, 8).toBigDecimal().doubleValue();
        double sma21 = row.getDecimal(5, 18, 8).toBigDecimal().doubleValue();
        String signal = row.getString(6).toString();
        double signalStrength = row.getDecimal(7, 5, 2).toBigDecimal().doubleValue();

        return new StrategySignal(runId, symbol, timestamp, close, sma5, sma21, signal, signalStrength);
    }
}
