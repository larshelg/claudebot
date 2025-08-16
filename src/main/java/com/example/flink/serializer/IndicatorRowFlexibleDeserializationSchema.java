package com.example.flink.serializer;

import com.alibaba.fluss.flink.source.deserializer.FlussDeserializationSchema;
import com.alibaba.fluss.record.LogRecord;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.types.RowType;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.util.*;

public class IndicatorRowFlexibleDeserializationSchema
        implements FlussDeserializationSchema<IndicatorRowFlexible> {

    private final List<String> projectedColumns; // ordered list of columns requested
    private final int symbolIndex;
    private final int timestampIndex;
    private final Map<String, Integer> indicatorColumnIndices = new HashMap<>();

    /**
     * @param projectedColumns The ordered columns requested from Fluss (must include "symbol" and "time")
     */
    public IndicatorRowFlexibleDeserializationSchema(List<String> projectedColumns) {
        this.projectedColumns = projectedColumns;
        this.symbolIndex = projectedColumns.indexOf("symbol");
        this.timestampIndex = projectedColumns.indexOf("time");

        for (int i = 0; i < projectedColumns.size(); i++) {
            String col = projectedColumns.get(i);
            if (!col.equals("symbol") && !col.equals("time")) {
                indicatorColumnIndices.put(col, i);
            }
        }
    }

    @Override
    public void open(FlussDeserializationSchema.InitializationContext initializationContext) throws Exception {

    }

    @Override
    public IndicatorRowFlexible deserialize(LogRecord record) {
        InternalRow row = record.getRow();

        String symbol = row.getString(symbolIndex).toString();
        long timestamp = row.getTimestampNtz(timestampIndex, 3).getMillisecond();

        Map<String, Double> indicators = new HashMap<>();
        for (Map.Entry<String, Integer> entry : indicatorColumnIndices.entrySet()) {
            String colName = entry.getKey();
            int idx = entry.getValue();
            // Safely check nulls
            if (!row.isNullAt(idx)) {
                indicators.put(colName, row.getDouble(idx));
            }
        }

        return new IndicatorRowFlexible(symbol, timestamp, indicators);
    }

    @Override
    public TypeInformation<IndicatorRowFlexible> getProducedType(RowType rowSchema) {
        return TypeInformation.of(IndicatorRowFlexible.class);
    }

}

