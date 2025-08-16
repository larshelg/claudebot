package com.example.flink.serializer;

import com.alibaba.fluss.flink.source.deserializer.FlussDeserializationSchema;
import com.alibaba.fluss.record.LogRecord;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.types.RowType;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CandleFlexibleDeserializationSchema
        implements FlussDeserializationSchema<CandleFlexible> {

    private final List<String> projectedColumns; // ordered list of requested columns
    private final int symbolIndex;
    private final int timestampIndex;
    private final Map<String, Integer> numericColumnIndices = new HashMap<>();
    private final Integer volumeIndex;

    public CandleFlexibleDeserializationSchema(List<String> projectedColumns) {
        this.projectedColumns = projectedColumns;
        this.symbolIndex = projectedColumns.indexOf("symbol");
        this.timestampIndex = projectedColumns.indexOf("time");

        Integer vIndex = null;
        for (int i = 0; i < projectedColumns.size(); i++) {
            String col = projectedColumns.get(i);
            if (col.equals("volume")) {
                vIndex = i;
            } else if (!col.equals("symbol") && !col.equals("time")) {
                numericColumnIndices.put(col, i);
            }
        }
        this.volumeIndex = vIndex;
    }

    @Override
    public void open(InitializationContext initializationContext) {
        // No-op, hook if you want metrics, serializers, etc.
    }

    @Override
    public CandleFlexible deserialize(LogRecord record) {
        InternalRow row = record.getRow();

        // Required fields
        String symbol = row.getString(symbolIndex).toString();
        long timestamp = row.getTimestampNtz(timestampIndex, 3).getMillisecond();

        // Collect numeric fields (OHLC, etc.)
        Map<String, Double> fields = new HashMap<>();
        for (Map.Entry<String, Integer> entry : numericColumnIndices.entrySet()) {
            int idx = entry.getValue();
            if (!row.isNullAt(idx)) {
                fields.put(entry.getKey(),
                        row.getDecimal(idx, 10, 4).toBigDecimal().doubleValue());
            }
        }

        // Optional volume
        Long volume = null;
        if (volumeIndex != null && !row.isNullAt(volumeIndex)) {
            volume = row.getLong(volumeIndex);
        }

        return new CandleFlexible(symbol, timestamp, fields, volume);
    }

    @Override
    public TypeInformation<CandleFlexible> getProducedType(RowType rowSchema) {
        return TypeInformation.of(CandleFlexible.class);
    }
}
