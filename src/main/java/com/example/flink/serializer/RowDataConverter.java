package com.example.flink.serializer;

import com.example.flink.StrategySignal;
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
}
