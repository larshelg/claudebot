package com.example.flink.indicators;

import com.example.flink.domain.Candle;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Detects swing highs/lows (a.k.a. fractals) per symbol in streaming fashion.
 *
 * Definition (left,right):
 * A bar i is a swing HIGH if its high is >= all highs of the previous 'left'
 * bars
 * and >= all highs of the next 'right' bars. (Analogous for LOW with lows.)
 *
 * Implementation notes:
 * - Requires 'right' lookahead: we buffer the last (left+right+1) bars per key.
 * - Emits a SwingPoint when the center bar becomes decidable.
 * - Optional minDeltaFrac filters tiny wiggles (vs. local price span).
 */
public final class SwingPointDetectorJob {

    private SwingPointDetectorJob() {
    }

    /** Output swing point. */
    public static class SwingPoint implements Serializable {
        public String symbol;
        public long ts; // timestamp of the center bar
        public String type; // "HIGH" or "LOW"
        public double price; // high or low of center bar
        public int left; // parameters used
        public int right; // parameters used
        public int rank; // simple rank = min(#strict-left, #strict-right)

        public static SwingPoint of(String sym, long ts, String type, double price, int left, int right, int rank) {
            SwingPoint s = new SwingPoint();
            s.symbol = sym;
            s.ts = ts;
            s.type = type;
            s.price = price;
            s.left = left;
            s.right = right;
            s.rank = rank;
            return s;
        }
    }

    /**
     * Build detector.
     *
     * @param bars         OHLCV stream (event-time)
     * @param lateness     watermark out-of-orderness
     * @param left         #bars to the left
     * @param right        #bars to the right (lookahead), >=1 recommended
     * @param minDeltaFrac discard swings with amplitude < (minDeltaFrac * local
     *                     span) [0..1], e.g. 0.02
     */
    public static DataStream<SwingPoint> build(DataStream<Candle> bars,
            Duration lateness,
            int left,
            int right,
            double minDeltaFrac) {
        var withTs = bars.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Candle>forBoundedOutOfOrderness(lateness)
                        .withTimestampAssigner((b, ts) -> b.timestamp));

        return withTs
                .keyBy(b -> b.symbol)
                .process(new SwingFn(left, right, minDeltaFrac))
                .name("SwingPointDetector");
    }

    // === operator ===
    static final class SwingFn extends KeyedProcessFunction<String, Candle, SwingPoint> {
        private final int L, R;
        private final double minDeltaFrac;

        private transient ListState<Candle> bufState; // ring of last (L+R+1) bars

        SwingFn(int left, int right, double minDeltaFrac) {
            this.L = Math.max(1, left);
            this.R = Math.max(1, right);
            this.minDeltaFrac = Math.max(0.0, minDeltaFrac);
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            bufState = getRuntimeContext().getListState(
                    new ListStateDescriptor<>("buf", Types.POJO(Candle.class)));
        }

        @Override
        public void processElement(Candle b, Context ctx, Collector<SwingPoint> out) throws Exception {
            // load existing buffer for this key
            Deque<Candle> buf = new ArrayDeque<>(L + R + 4);
            for (Candle prev : bufState.get()) {
                buf.addLast(prev);
            }
            // append bar
            buf.addLast(b);
            if (buf.size() > L + R + 1) {
                // keep fixed-size window: drop oldest
                buf.removeFirst();
            }
            // persist state (cheap)
            bufState.update(new java.util.ArrayList<>(buf));

            if (buf.size() < L + R + 1)
                return; // need enough lookahead

            // decide center at index L (0-based)
            Candle[] arr = buf.toArray(new Candle[0]);
            Candle center = arr[L];

            // local span for amplitude filter
            double hi = Double.NEGATIVE_INFINITY, lo = Double.POSITIVE_INFINITY;
            for (Candle x : arr) {
                if (x.high > hi)
                    hi = x.high;
                if (x.low < lo)
                    lo = x.low;
            }
            double span = Math.max(1e-9, hi - lo);

            // check high swing
            boolean highOk = true;
            int stricterLeftHigh = 0, stricterRightHigh = 0;
            for (int i = 0; i < L; i++)
                if (center.high < arr[i].high)
                    highOk = false;
                else if (center.high > arr[i].high)
                    stricterLeftHigh++;
            for (int i = L + 1; i < L + 1 + R; i++)
                if (center.high < arr[i].high)
                    highOk = false;
                else if (center.high > arr[i].high)
                    stricterRightHigh++;

            // check low swing
            boolean lowOk = true;
            int stricterLeftLow = 0, stricterRightLow = 0;
            for (int i = 0; i < L; i++)
                if (center.low > arr[i].low)
                    lowOk = false;
                else if (center.low < arr[i].low)
                    stricterLeftLow++;
            for (int i = L + 1; i < L + 1 + R; i++)
                if (center.low > arr[i].low)
                    lowOk = false;
                else if (center.low < arr[i].low)
                    stricterRightLow++;

            // amplitude filter
            boolean passesHighAmp = (center.high - lo) >= (minDeltaFrac * span);
            boolean passesLowAmp = (hi - center.low) >= (minDeltaFrac * span);

            if (highOk && passesHighAmp) {
                int rank = Math.min(stricterLeftHigh, stricterRightHigh);
                out.collect(SwingPoint.of(center.symbol, center.timestamp, "HIGH", center.high, L, R, rank));
            }
            if (lowOk && passesLowAmp) {
                int rank = Math.min(stricterLeftLow, stricterRightLow);
                out.collect(SwingPoint.of(center.symbol, center.timestamp, "LOW", center.low, L, R, rank));
            }
        }
    }
}
