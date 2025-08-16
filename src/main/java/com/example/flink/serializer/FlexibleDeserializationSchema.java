package com.example.flink.serializer;

import com.alibaba.fluss.flink.source.deserializer.FlussDeserializationSchema;
import com.alibaba.fluss.record.LogRecord;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.types.RowType;

import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Generic Flexible Deserialization Schema for Fluss.
 *
 * T = target POJO type
 */
public class FlexibleDeserializationSchema<T> implements FlussDeserializationSchema<T> {

    private final List<String> projectedColumns;
    private final int timestampIndex;
    private final int symbolIndex;
    private final Map<String, Integer> numericColumnIndices = new HashMap<>();
    private final Integer specialIndex; // optional field like volume
    private final Function<DeserializedRow, T> mapper;

    /**
     * @param projectedColumns List of requested columns in table order
     * @param mapper Function that converts DeserializedRow -> target POJO
     *               DeserializedRow has: symbol, timestamp, numericFields map, optional special field
     */
    public FlexibleDeserializationSchema(List<String> projectedColumns, Function<DeserializedRow, T> mapper) {
        this.projectedColumns = projectedColumns;
        this.timestampIndex = projectedColumns.indexOf("time");
        this.symbolIndex = projectedColumns.indexOf("symbol");

        Integer sIndex = null;
        for (int i = 0; i < projectedColumns.size(); i++) {
            String col = projectedColumns.get(i);
            if (!col.equals("symbol") && !col.equals("time")) {
                if (col.equals("volume")) {
                    sIndex = i;
                } else {
                    numericColumnIndices.put(col, i);
                }
            }
        }
        this.specialIndex = sIndex;
        this.mapper = mapper;
    }

    @Override
    public void open(InitializationContext context) {
        // no-op
    }

    @Override
    public T deserialize(LogRecord record) {
        InternalRow row = record.getRow();

        String symbol = row.getString(symbolIndex).toString();
        long timestamp = row.getTimestampNtz(timestampIndex, 3).getMillisecond();

        Map<String, Double> numericFields = new HashMap<>();
        for (Map.Entry<String, Integer> entry : numericColumnIndices.entrySet()) {
            int idx = entry.getValue();
            if (!row.isNullAt(idx)) {
                numericFields.put(entry.getKey(), row.getDecimal(idx, 10, 4).toBigDecimal().doubleValue());
            }
        }

        Object specialValue = null;
        if (specialIndex != null && !row.isNullAt(specialIndex)) {
            // Try as Long first, fallback to Decimal if needed
            try {
                specialValue = row.getLong(specialIndex);
            } catch (ClassCastException e) {
                specialValue = row.getDecimal(specialIndex, 10, 4).toBigDecimal().longValue();
            }
        }

        DeserializedRow dr = new DeserializedRow(symbol, timestamp, numericFields, specialValue);
        return mapper.apply(dr);
    }

    @Override
    public TypeInformation<T> getProducedType(RowType rowType) {
        return TypeInformation.of((Class<T>) Object.class);
    }

    /** Holder for deserialized values before mapping to POJO */
    public static class DeserializedRow {
        public final String symbol;
        public final long timestamp;
        public final Map<String, Double> indicators;
        public final Object special;

        public DeserializedRow(String symbol, long timestamp, Map<String, Double> indicators, Object special) {
            this.symbol = symbol;
            this.timestamp = timestamp;
            this.indicators = indicators;
            this.special = special;
        }

        public Long getSpecialAsLong() {
            return special == null ? null : (Long) special;
        }

        public Double getIndicator(String name) {
            return indicators.get(name);
        }
    }
}
