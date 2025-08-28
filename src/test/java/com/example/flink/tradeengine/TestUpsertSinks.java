package com.example.flink.tradeengine;

import com.example.flink.domain.Position;
import com.example.flink.domain.Portfolio;
import com.example.flink.domain.RealizedPnl;
import com.example.flink.domain.UnrealizedPnl;
import com.example.flink.domain.PositionClose;
import com.example.flink.domain.TradeMatch;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Test-only sinks that emulate Fluss upsert semantics using an in-memory map.
 * The map key is chosen to match the table primary key of the "latest" views.
 */
public class TestUpsertSinks {

    public static class PositionLatestSink implements Sink<Position> {
        private static final Map<String, Position> latest = new ConcurrentHashMap<>();

        public static void clear() {
            latest.clear();
        }

        public static List<Position> getResults() {
            return new ArrayList<>(latest.values());
        }

        public static Position get(String accountId, String symbol) {
            return latest.get(accountId + "|" + symbol);
        }

        @Override
        public SinkWriter<Position> createWriter(InitContext context) {
            return new SinkWriter<Position>() {
                @Override
                public void write(Position element, Context context) {
                    String key = element.accountId + "|" + element.symbol;
                    if (Math.abs(element.netQty) == 0.0) {
                        latest.remove(key);
                    } else {
                        latest.put(key, copyPosition(element));
                    }
                }

                @Override
                public void flush(boolean endOfInput) {
                }

                @Override
                public void close() {
                }
            };
        }

        private Position copyPosition(Position pos) {
            Position copy = new Position();
            copy.accountId = pos.accountId;
            copy.symbol = pos.symbol;
            copy.netQty = pos.netQty;
            copy.avgPrice = pos.avgPrice;
            copy.lastUpdated = pos.lastUpdated;
            return copy;
        }
    }

    // Append sink for Position Close events (history)
    public static class PositionCloseHistorySink implements Sink<PositionClose> {
        private static final List<PositionClose> history = new java.util.concurrent.CopyOnWriteArrayList<>();

        public static void clear() {
            history.clear();
        }

        public static List<PositionClose> getResults() {
            return new ArrayList<>(history);
        }

        @Override
        public SinkWriter<PositionClose> createWriter(InitContext context) {
            return new SinkWriter<PositionClose>() {
                @Override
                public void write(PositionClose element, Context context) {
                    history.add(copy(element));
                }

                @Override
                public void flush(boolean endOfInput) {
                }

                @Override
                public void close() {
                }
            };
        }

        private PositionClose copy(PositionClose pc) {
            return new PositionClose(pc.accountId, pc.symbol, pc.totalQty, pc.avgPrice, pc.openTs, pc.closeTs,
                    pc.realizedPnl);
        }
    }

    // Append sink for Trade Match events (history)
    public static class TradeMatchHistorySink implements Sink<TradeMatch> {
        private static final List<TradeMatch> history = new java.util.concurrent.CopyOnWriteArrayList<>();

        public static void clear() {
            history.clear();
        }

        public static List<TradeMatch> getResults() {
            return new ArrayList<>(history);
        }

        @Override
        public SinkWriter<TradeMatch> createWriter(InitContext context) {
            return new SinkWriter<TradeMatch>() {
                @Override
                public void write(TradeMatch element, Context context) {
                    history.add(copy(element));
                }

                @Override
                public void flush(boolean endOfInput) {
                }

                @Override
                public void close() {
                }
            };
        }

        private TradeMatch copy(TradeMatch tm) {
            TradeMatch copy = new TradeMatch();
            copy.matchId = tm.matchId;
            copy.accountId = tm.accountId;
            copy.symbol = tm.symbol;
            copy.buyOrderId = tm.buyOrderId;
            copy.sellOrderId = tm.sellOrderId;
            copy.matchedQty = tm.matchedQty;
            copy.buyPrice = tm.buyPrice;
            copy.sellPrice = tm.sellPrice;
            copy.realizedPnl = tm.realizedPnl;
            copy.matchTimestamp = tm.matchTimestamp;
            copy.buyTimestamp = tm.buyTimestamp;
            copy.sellTimestamp = tm.sellTimestamp;
            return copy;
        }
    }

    public static class PortfolioLatestSink implements Sink<Portfolio> {
        private static final Map<String, Portfolio> latest = new ConcurrentHashMap<>();

        public static void clear() {
            latest.clear();
        }

        public static List<Portfolio> getResults() {
            return new ArrayList<>(latest.values());
        }

        public static Portfolio get(String accountId) {
            return latest.get(accountId);
        }

        @Override
        public SinkWriter<Portfolio> createWriter(InitContext context) {
            return new SinkWriter<Portfolio>() {
                @Override
                public void write(Portfolio element, Context context) {
                    latest.put(element.accountId, copyPortfolio(element));
                }

                @Override
                public void flush(boolean endOfInput) {
                }

                @Override
                public void close() {
                }
            };
        }

        private Portfolio copyPortfolio(Portfolio pf) {
            Portfolio copy = new Portfolio();
            copy.accountId = pf.accountId;
            copy.equity = pf.equity;
            copy.cashBalance = pf.cashBalance;
            copy.exposure = pf.exposure;
            copy.marginUsed = pf.marginUsed;
            return copy;
        }
    }

    public static class RealizedPnlLatestSink implements Sink<RealizedPnl> {
        private static final Map<String, RealizedPnl> latest = new ConcurrentHashMap<>();

        public static void clear() {
            latest.clear();
        }

        public static List<RealizedPnl> getResults() {
            return new ArrayList<>(latest.values());
        }

        public static RealizedPnl get(String accountId, String symbol) {
            return latest.get(accountId + "|" + symbol);
        }

        @Override
        public SinkWriter<RealizedPnl> createWriter(InitContext context) {
            return new SinkWriter<RealizedPnl>() {
                @Override
                public void write(RealizedPnl element, Context context) {
                    String key = element.accountId + "|" + element.symbol;
                    latest.put(key, copyRealized(element));
                }

                @Override
                public void flush(boolean endOfInput) {
                }

                @Override
                public void close() {
                }
            };
        }

        private RealizedPnl copyRealized(RealizedPnl r) {
            return new RealizedPnl(r.accountId, r.symbol, r.realizedPnl, r.ts);
        }
    }

    public static class UnrealizedPnlLatestSink implements Sink<UnrealizedPnl> {
        private static final Map<String, UnrealizedPnl> latest = new ConcurrentHashMap<>();

        public static void clear() {
            latest.clear();
        }

        public static List<UnrealizedPnl> getResults() {
            return new ArrayList<>(latest.values());
        }

        public static UnrealizedPnl get(String accountId, String symbol) {
            return latest.get(accountId + "|" + symbol);
        }

        @Override
        public SinkWriter<UnrealizedPnl> createWriter(InitContext context) {
            return new SinkWriter<UnrealizedPnl>() {
                @Override
                public void write(UnrealizedPnl element, Context context) {
                    String key = element.accountId + "|" + element.symbol;
                    latest.put(key, copyUnrealized(element));
                }

                @Override
                public void flush(boolean endOfInput) {
                }

                @Override
                public void close() {
                }
            };
        }

        private UnrealizedPnl copyUnrealized(UnrealizedPnl u) {
            return new UnrealizedPnl(u.accountId, u.symbol, u.unrealizedPnl, u.currentPrice, u.avgPrice, u.netQty,
                    u.ts);
        }
    }
}
