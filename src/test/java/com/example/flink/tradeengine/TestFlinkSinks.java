package com.example.flink.tradeengine;

import com.example.flink.domain.ExecReport;
import com.example.flink.domain.Portfolio;
import com.example.flink.domain.Position;
import com.example.flink.domain.RiskAlert;
import com.example.flink.domain.TradeMatch;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class TestFlinkSinks {

    public static class TestPositionSink implements Sink<Position> {
        private static final Queue<Position> collectedPositions = new ConcurrentLinkedQueue<>();

        public static void clear() {
            collectedPositions.clear();
        }

        public static List<Position> getResults() {
            return new ArrayList<>(collectedPositions);
        }

        @Override
        public SinkWriter<Position> createWriter(InitContext context) {
            return new SinkWriter<Position>() {
                @Override
                public void write(Position element, Context context) {
                    collectedPositions.add(copy(element));
                }

                @Override
                public void flush(boolean endOfInput) {
                }

                @Override
                public void close() {
                }
            };
        }

        private Position copy(Position pos) {
            Position copy = new Position();
            copy.accountId = pos.accountId;
            copy.symbol = pos.symbol;
            copy.netQty = pos.netQty;
            copy.avgPrice = pos.avgPrice;
            copy.lastUpdated = pos.lastUpdated;
            return copy;
        }
    }

    public static class TestPortfolioSink implements Sink<Portfolio> {
        private static final Queue<Portfolio> collectedPortfolios = new ConcurrentLinkedQueue<>();

        public static void clear() {
            collectedPortfolios.clear();
        }

        public static List<Portfolio> getResults() {
            return new ArrayList<>(collectedPortfolios);
        }

        @Override
        public SinkWriter<Portfolio> createWriter(InitContext context) {
            return new SinkWriter<Portfolio>() {
                @Override
                public void write(Portfolio element, Context context) {
                    collectedPortfolios.add(copy(element));
                }

                @Override
                public void flush(boolean endOfInput) {
                }

                @Override
                public void close() {
                }
            };
        }

        private Portfolio copy(Portfolio pf) {
            Portfolio copy = new Portfolio();
            copy.accountId = pf.accountId;
            copy.equity = pf.equity;
            copy.cashBalance = pf.cashBalance;
            copy.exposure = pf.exposure;
            copy.marginUsed = pf.marginUsed;
            return copy;
        }
    }

    public static class TestRiskAlertSink implements Sink<RiskAlert> {
        private static final Queue<RiskAlert> collectedAlerts = new ConcurrentLinkedQueue<>();

        public static void clear() {
            collectedAlerts.clear();
        }

        public static List<RiskAlert> getResults() {
            return new ArrayList<>(collectedAlerts);
        }

        @Override
        public SinkWriter<RiskAlert> createWriter(InitContext context) {
            return new SinkWriter<RiskAlert>() {
                @Override
                public void write(RiskAlert element, Context context) {
                    collectedAlerts.add(copy(element));
                }

                @Override
                public void flush(boolean endOfInput) {
                }

                @Override
                public void close() {
                }
            };
        }

        private RiskAlert copy(RiskAlert alert) {
            RiskAlert copy = new RiskAlert();
            copy.accountId = alert.accountId;
            copy.message = alert.message;
            return copy;
        }
    }

    public static class TestExecReportSink implements Sink<ExecReport> {
        private static final Queue<ExecReport> collectedExecReports = new ConcurrentLinkedQueue<>();

        public static void clear() {
            collectedExecReports.clear();
        }

        public static List<ExecReport> getResults() {
            return new ArrayList<>(collectedExecReports);
        }

        @Override
        public SinkWriter<ExecReport> createWriter(InitContext context) {
            return new SinkWriter<ExecReport>() {
                @Override
                public void write(ExecReport element, Context context) {
                    collectedExecReports.add(copy(element));
                }

                @Override
                public void flush(boolean endOfInput) {
                }

                @Override
                public void close() {
                }
            };
        }

        private ExecReport copy(ExecReport exec) {
            return new ExecReport(exec.accountId, exec.orderId, exec.symbol, exec.fillQty, exec.fillPrice, exec.status,
                    exec.ts);
        }
    }

    public static class TestTradeMatchSink implements Sink<TradeMatch> {
        private static final Queue<TradeMatch> collectedTradeMatches = new ConcurrentLinkedQueue<>();

        public static void clear() {
            collectedTradeMatches.clear();
        }

        public static List<TradeMatch> getResults() {
            return new ArrayList<>(collectedTradeMatches);
        }

        @Override
        public SinkWriter<TradeMatch> createWriter(InitContext context) {
            return new SinkWriter<TradeMatch>() {
                @Override
                public void write(TradeMatch element, Context context) {
                    collectedTradeMatches.add(copy(element));
                }

                @Override
                public void flush(boolean endOfInput) {
                }

                @Override
                public void close() {
                }
            };
        }

        private TradeMatch copy(TradeMatch match) {
            TradeMatch copy = new TradeMatch();
            copy.matchId = match.matchId;
            copy.accountId = match.accountId;
            copy.symbol = match.symbol;
            copy.buyOrderId = match.buyOrderId;
            copy.sellOrderId = match.sellOrderId;
            copy.matchedQty = match.matchedQty;
            copy.buyPrice = match.buyPrice;
            copy.sellPrice = match.sellPrice;
            copy.realizedPnl = match.realizedPnl;
            copy.matchTimestamp = match.matchTimestamp;
            copy.buyTimestamp = match.buyTimestamp;
            copy.sellTimestamp = match.sellTimestamp;
            return copy;
        }
    }

    public static class TestTrackingSinkFactory implements TrackingSinkFactory {
        @Override
        public Sink<ExecReport> createExecReportSink() {
            return new TestExecReportSink();
        }

        @Override
        public Sink<TradeMatch> createTradeMatchSink() {
            return new TestTradeMatchSink();
        }
    }
}
