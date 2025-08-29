package com.example.flink.tradeengine;

import com.example.flink.helpers.NoopSink;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.streaming.api.datastream.*;

import java.io.Serializable;

import com.example.flink.domain.AccountPolicy;
import com.example.flink.domain.ExecReport;
import com.example.flink.domain.Portfolio;
import com.example.flink.domain.Position;
import com.example.flink.domain.PositionClose;
import com.example.flink.domain.RiskAlert;
import com.example.flink.domain.TradeSignal;
import com.example.flink.domain.TradeMatch;
import com.example.flink.domain.RealizedPnl;
import com.example.flink.domain.PriceTick;
import com.example.flink.domain.UnrealizedPnl;

public class PortfolioAndRiskJob implements Serializable {
    private final DataStream<ExecReport> execReports;
    private final DataStream<AccountPolicy> accountPolicies;
    private Sink<Position> positionSink;
    private Sink<Portfolio> portfolioSink;
    private Sink<RiskAlert> riskAlertSink;
    private Sink<TradeMatch> tradeMatchSink; // optional external sink for history
    private Sink<RealizedPnl> realizedPnlSink; // optional external sink for history
    private Sink<UnrealizedPnl> unrealizedPnlSink; // optional external sink for open P&L
    private Sink<PositionClose> positionCloseSink; // optional append sink for close events

    public Sink<Position> getPositionSink() {
        return positionSink;
    }

    public void setPositionSink(Sink<Position> positionSink) {
        this.positionSink = positionSink;
    }

    public Sink<Portfolio> getPortfolioSink() {
        return portfolioSink;
    }

    public void setPortfolioSink(Sink<Portfolio> portfolioSink) {
        this.portfolioSink = portfolioSink;
    }

    public Sink<RiskAlert> getRiskAlertSink() {
        return riskAlertSink;
    }

    public void setRiskAlertSink(Sink<RiskAlert> riskAlertSink) {
        this.riskAlertSink = riskAlertSink;
    }

    public Sink<TradeMatch> getTradeMatchSink() {
        return tradeMatchSink;
    }

    public void setTradeMatchSink(Sink<TradeMatch> tradeMatchSink) {
        this.tradeMatchSink = tradeMatchSink;
    }

    public Sink<RealizedPnl> getRealizedPnlSink() {
        return realizedPnlSink;
    }

    public void setRealizedPnlSink(Sink<RealizedPnl> realizedPnlSink) {
        this.realizedPnlSink = realizedPnlSink;
    }

    public Sink<UnrealizedPnl> getUnrealizedPnlSink() {
        return unrealizedPnlSink;
    }

    public void setUnrealizedPnlSink(Sink<UnrealizedPnl> unrealizedPnlSink) {
        this.unrealizedPnlSink = unrealizedPnlSink;
    }

    public Sink<PositionClose> getPositionCloseSink() {
        return positionCloseSink;
    }

    public void setPositionCloseSink(Sink<PositionClose> positionCloseSink) {
        this.positionCloseSink = positionCloseSink;
    }

    public PortfolioAndRiskJob(DataStream<TradeSignal> tradeSignals, DataStream<ExecReport> execReports,
            DataStream<AccountPolicy> accountPolicies) {
        this.execReports = execReports;
        this.accountPolicies = accountPolicies;
        this.positionSink = new NoopSink<Position>();
        this.portfolioSink = new NoopSink<Portfolio>();
        this.riskAlertSink = new NoopSink<RiskAlert>();
        this.tradeMatchSink = new NoopSink<TradeMatch>();
        this.realizedPnlSink = new NoopSink<RealizedPnl>();
        this.unrealizedPnlSink = new NoopSink<UnrealizedPnl>();
    }

    // Overload: consume only ExecReports and AccountPolicy (TradeSignals already
    // consumed upstream)
    public PortfolioAndRiskJob(DataStream<ExecReport> execReports, DataStream<AccountPolicy> accountPolicies) {
        this.execReports = execReports;
        this.accountPolicies = accountPolicies;
        this.positionSink = new NoopSink<Position>();
        this.portfolioSink = new NoopSink<Portfolio>();
        this.riskAlertSink = new NoopSink<RiskAlert>();
        this.tradeMatchSink = new NoopSink<TradeMatch>();
        this.realizedPnlSink = new NoopSink<RealizedPnl>();
        this.unrealizedPnlSink = new NoopSink<UnrealizedPnl>();
    }

    /**
     * Holder for computed streams so callers (tests/cluster jobs) can attach sinks
     * externally.
     */
    public static class Results {
        public final SingleOutputStreamOperator<Position> positions;
        public final DataStream<Portfolio> portfolios;
        public final DataStream<RiskAlert> riskAlerts;
        public final DataStream<TradeMatch> tradeMatches;
        public final DataStream<RealizedPnl> realizedPnl;
        public final DataStream<UnrealizedPnl> unrealizedPnl;
        public final DataStream<PositionClose> positionCloses;

        public Results(SingleOutputStreamOperator<Position> positions,
                DataStream<Portfolio> portfolios,
                DataStream<RiskAlert> riskAlerts,
                DataStream<TradeMatch> tradeMatches,
                DataStream<RealizedPnl> realizedPnl,
                DataStream<UnrealizedPnl> unrealizedPnl,
                DataStream<PositionClose> positionCloses) {
            this.positions = positions;
            this.portfolios = portfolios;
            this.riskAlerts = riskAlerts;
            this.tradeMatches = tradeMatches;
            this.realizedPnl = realizedPnl;
            this.unrealizedPnl = unrealizedPnl;
            this.positionCloses = positionCloses;
        }
    }

    /**
     * Optional sinks for cluster mode. Null means no sink will be attached here.
     */
    public static class Sinks {
        public final Sink<Position> positionSink;
        public final Sink<Portfolio> portfolioSink;
        public final Sink<RiskAlert> riskAlertSink;
        public final Sink<TradeMatch> tradeMatchSink;
        public final Sink<RealizedPnl> realizedPnlSink;
        public final Sink<UnrealizedPnl> unrealizedPnlSink;
        public final Sink<PositionClose> positionCloseSink;

        public Sinks(Sink<Position> positionSink,
                Sink<Portfolio> portfolioSink,
                Sink<RiskAlert> riskAlertSink,
                Sink<TradeMatch> tradeMatchSink,
                Sink<RealizedPnl> realizedPnlSink,
                Sink<UnrealizedPnl> unrealizedPnlSink,
                Sink<PositionClose> positionCloseSink) {
            this.positionSink = positionSink;
            this.portfolioSink = portfolioSink;
            this.riskAlertSink = riskAlertSink;
            this.tradeMatchSink = tradeMatchSink;
            this.realizedPnlSink = realizedPnlSink;
            this.unrealizedPnlSink = unrealizedPnlSink;
            this.positionCloseSink = positionCloseSink;
        }
    }

    /**
     * Build the portfolio/risk pipeline from ExecReports and AccountPolicy.
     * Optionally attach sinks.
     */
    public static Results processExecReports(DataStream<ExecReport> execReports,
            DataStream<AccountPolicy> accountPolicies,
            Sinks sinksOrNull) {
        // Positions from exec reports
        SingleOutputStreamOperator<Position> positions = execReports
                .keyBy(r -> r.accountId + "|" + r.symbol)
                .process(new PositionUpdater());

        // Side output: position closes
        DataStream<PositionClose> positionCloses = positions.getSideOutput(PositionUpdater.POSITION_CLOSE_TAG);

        // FIFO trade matching to compute realized P&L for closed portions
        DataStream<TradeMatch> tradeMatches = execReports
                .keyBy(r -> r.accountId + "|" + r.symbol)
                .process(new FIFOTradeMatchingEngine());

        // Aggregate realized PnL per account-symbol from trade matches
        DataStream<RealizedPnl> realizedPnl = tradeMatches
                .keyBy(m -> m.accountId + "|" + m.symbol)
                .process(new RealizedPnlAggregator());

        // Derive price ticks from execution reports (proxy for market prices)
        DataStream<PriceTick> priceTicks = execReports
                .map(er -> new PriceTick(er.accountId, er.symbol, er.fillPrice, er.ts))
                .returns(PriceTick.class);

        // Compute unrealized PnL for open positions using last seen price
        DataStream<UnrealizedPnl> unrealizedPnl = positions
                .keyBy(p -> p.accountId + "|" + p.symbol)
                .connect(priceTicks.keyBy(pt -> pt.accountId + "|" + pt.symbol))
                .process(new UnrealizedPnlCalculator());

        // Portfolio and risk alerts
        DataStream<Portfolio> portfolios = positions
                .keyBy(p -> p.accountId)
                .connect(accountPolicies.keyBy(p -> p.accountId))
                .process(new PortfolioUpdater());

        DataStream<RiskAlert> riskAlerts = portfolios.process(new RiskEngine());

        // Attach sinks if provided; otherwise print for debugging
        if (sinksOrNull != null) {
            if (sinksOrNull.positionSink != null) {
                positions.sinkTo(sinksOrNull.positionSink);
            } else {
                positions.print("POS");
            }
            if (sinksOrNull.portfolioSink != null) {
                portfolios.sinkTo(sinksOrNull.portfolioSink);
            } else {
                portfolios.print("PF");
            }
            if (sinksOrNull.riskAlertSink != null) {
                riskAlerts.sinkTo(sinksOrNull.riskAlertSink);
            } else {
                riskAlerts.print("RISK");
            }
            if (sinksOrNull.positionCloseSink != null) {
                positionCloses.sinkTo(sinksOrNull.positionCloseSink);
            } else {
                positionCloses.print("POS_CLOSE");
            }
            if (sinksOrNull.tradeMatchSink != null) {
                tradeMatches.sinkTo(sinksOrNull.tradeMatchSink);
            } else {
                tradeMatches.print("MATCH");
            }
            if (sinksOrNull.realizedPnlSink != null) {
                realizedPnl.sinkTo(sinksOrNull.realizedPnlSink);
            } else {
                realizedPnl.print("REALIZED");
            }
            if (sinksOrNull.unrealizedPnlSink != null) {
                unrealizedPnl.sinkTo(sinksOrNull.unrealizedPnlSink);
            } else {
                unrealizedPnl.print("UNREAL");
            }
        }

        return new Results(positions, portfolios, riskAlerts, tradeMatches, realizedPnl, unrealizedPnl, positionCloses);
    }

    public void run() throws Exception {
        // Route through the static processor using the instance sinks
        processExecReports(this.execReports, this.accountPolicies, new Sinks(
                this.positionSink,
                this.portfolioSink,
                this.riskAlertSink,
                this.tradeMatchSink,
                this.realizedPnlSink,
                this.unrealizedPnlSink,
                this.positionCloseSink));
    }

}
