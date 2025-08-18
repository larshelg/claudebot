package com.example.flink;

import com.example.flink.domain.ExecReport;
import com.example.flink.domain.TradeSignal;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Collections;

class VenueOrder {
    public String accountId;
    public String orderId;
    public String symbol;
    public double qty;
    public double price;
    public String venue; // e.g. "BINANCE"
    public String status; // NEW / PARTIAL / FILLED / REJECTED
    public long ts;
}

// --- Main Job ---
public class ExecutionAdapterJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // --- Source ---
        // Placeholder: empty stream until Fluss source is wired
        DataStream<TradeSignal> tradeSignals = env.fromCollection(Collections.<TradeSignal>emptyList());

        // --- Process trade signals into venue orders ---
        DataStream<VenueOrder> orders = tradeSignals
                .keyBy(ts -> ts.accountId + "|" + ts.symbol)
                .process(new TradeSignalProcessor());

        // --- Sink ---
        // For now: log output
        orders.print("ORDERS");
        // Later: sink to Fluss orders_state / exec_reports / venue adapter
        // orders.addSink(fluxSinkOrders);

        env.execute("Job D - Execution Adapter");
    }

    // --- Stateful operator ---
    static class TradeSignalProcessor extends KeyedProcessFunction<String, TradeSignal, VenueOrder> {
        // Track pending orders per account+symbol
        private transient MapState<String, VenueOrder> pendingOrders;

        @Override
        public void open(Configuration parameters) {
            MapStateDescriptor<String, VenueOrder> descriptor = new MapStateDescriptor<>("pendingOrders", String.class,
                    VenueOrder.class);
            pendingOrders = getRuntimeContext().getMapState(descriptor);
        }

        @Override
        public void processElement(TradeSignal signal, Context ctx, Collector<VenueOrder> out) throws Exception {
            // Create a venue order
            VenueOrder order = new VenueOrder();
            order.accountId = signal.accountId;
            order.symbol = signal.symbol;
            order.qty = signal.qty;
            order.price = signal.price;
            order.venue = "SIM_EXCHANGE"; // placeholder
            order.status = "NEW";
            order.ts = System.currentTimeMillis();
            order.orderId = signal.accountId + "-" + signal.symbol + "-" + order.ts;

            // Store in pending orders
            pendingOrders.put(order.orderId, order);

            // Emit to downstream (would normally go to venue adapter)
            out.collect(order);

            // Later: handle async acks/fills from venue
        }

        // Optional: handle venue callbacks / timeouts
        public void processVenueCallback(ExecReport report) throws Exception {
            VenueOrder order = pendingOrders.get(report.orderId);
            if (order != null) {
                order.status = report.status;
                // remove if fully filled or rejected
                if ("FILLED".equals(report.status) || "REJECTED".equals(report.status)) {
                    pendingOrders.remove(report.orderId);
                }
            }
        }
    }
}
