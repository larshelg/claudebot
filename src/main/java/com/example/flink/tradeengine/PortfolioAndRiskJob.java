package com.example.flink.tradeengine;

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
    private final DataStream<TradeSignal> tradeSignals;
    private final DataStream<ExecReport> execReports;
    private final DataStream<AccountPolicy> accountPolicies;
    private final Sink<Position> positionSink;
    private final Sink<Portfolio> portfolioSink;
    private final Sink<RiskAlert> riskAlertSink;
    private final TrackingSinkFactory trackingSinkFactory;
    private Sink<TradeMatch> tradeMatchSink; // optional external sink for history
    private Sink<RealizedPnl> realizedPnlSink; // optional external sink for history
    private Sink<UnrealizedPnl> unrealizedPnlSink; // optional external sink for open P&L
    private Sink<PositionClose> positionCloseSink; // optional append sink for close events

    public PortfolioAndRiskJob(DataStream<TradeSignal> tradeSignals, DataStream<ExecReport> execReports) {
        this.tradeSignals = tradeSignals;
        this.execReports = execReports;
        this.accountPolicies = tradeSignals
                .map(ts -> new AccountPolicy(ts.accountId, 3, "ACTIVE", 100_000.0, ts.ts))
                .returns(AccountPolicy.class);
        this.positionSink = null;
        this.portfolioSink = null;
        this.riskAlertSink = null;
        this.trackingSinkFactory = null;
        this.tradeMatchSink = null;
        this.realizedPnlSink = null;
        this.unrealizedPnlSink = null;
        this.positionCloseSink = null;
    }

    public PortfolioAndRiskJob(DataStream<TradeSignal> tradeSignals,
            DataStream<ExecReport> execReports,
            DataStream<AccountPolicy> accountPolicies) {
        this.tradeSignals = tradeSignals;
        this.execReports = execReports;
        this.accountPolicies = accountPolicies;
        this.positionSink = null;
        this.portfolioSink = null;
        this.riskAlertSink = null;
        this.trackingSinkFactory = null;
        this.tradeMatchSink = null;
        this.realizedPnlSink = null;
        this.unrealizedPnlSink = null;
        this.positionCloseSink = null;
    }

    public PortfolioAndRiskJob(DataStream<TradeSignal> tradeSignals,
            DataStream<ExecReport> execReports,
            Sink<Position> positionSink,
            Sink<Portfolio> portfolioSink,
            Sink<RiskAlert> riskAlertSink) {
        this.tradeSignals = tradeSignals;
        this.execReports = execReports;
        this.accountPolicies = tradeSignals
                .map(ts -> new AccountPolicy(ts.accountId, 3, "ACTIVE", 100_000.0, ts.ts))
                .returns(AccountPolicy.class);
        this.positionSink = positionSink;
        this.portfolioSink = portfolioSink;
        this.riskAlertSink = riskAlertSink;
        this.trackingSinkFactory = null;
        this.tradeMatchSink = null;
        this.realizedPnlSink = null;
        this.unrealizedPnlSink = null;
        this.positionCloseSink = null;
    }

    public PortfolioAndRiskJob(DataStream<TradeSignal> tradeSignals,
            DataStream<ExecReport> execReports,
            DataStream<AccountPolicy> accountPolicies,
            Sink<Position> positionSink,
            Sink<Portfolio> portfolioSink,
            Sink<RiskAlert> riskAlertSink) {
        this.tradeSignals = tradeSignals;
        this.execReports = execReports;
        this.accountPolicies = accountPolicies;
        this.positionSink = positionSink;
        this.portfolioSink = portfolioSink;
        this.riskAlertSink = riskAlertSink;
        this.trackingSinkFactory = null;
    }

    public PortfolioAndRiskJob(DataStream<TradeSignal> tradeSignals,
            DataStream<ExecReport> execReports,
            DataStream<AccountPolicy> accountPolicies,
            Sink<Position> positionSink,
            Sink<Portfolio> portfolioSink,
            Sink<RiskAlert> riskAlertSink,
            TrackingSinkFactory trackingSinkFactory) {
        this.tradeSignals = tradeSignals;
        this.execReports = execReports;
        this.accountPolicies = accountPolicies;
        this.positionSink = positionSink;
        this.portfolioSink = portfolioSink;
        this.riskAlertSink = riskAlertSink;
        this.trackingSinkFactory = trackingSinkFactory;
        this.tradeMatchSink = null;
        this.realizedPnlSink = null;
        this.unrealizedPnlSink = null;
        this.positionCloseSink = null;
    }

    public PortfolioAndRiskJob(DataStream<TradeSignal> tradeSignals,
            DataStream<ExecReport> execReports,
            DataStream<AccountPolicy> accountPolicies,
            Sink<Position> positionSink,
            Sink<Portfolio> portfolioSink,
            Sink<RiskAlert> riskAlertSink,
            TrackingSinkFactory trackingSinkFactory,
            Sink<TradeMatch> tradeMatchSink,
            Sink<RealizedPnl> realizedPnlSink,
            Sink<UnrealizedPnl> unrealizedPnlSink) {
        this.tradeSignals = tradeSignals;
        this.execReports = execReports;
        this.accountPolicies = accountPolicies;
        this.positionSink = positionSink;
        this.portfolioSink = portfolioSink;
        this.riskAlertSink = riskAlertSink;
        this.trackingSinkFactory = trackingSinkFactory;
        this.tradeMatchSink = tradeMatchSink;
        this.realizedPnlSink = realizedPnlSink;
        this.unrealizedPnlSink = unrealizedPnlSink;
    }

    // Extended with PositionClose history sink
    public PortfolioAndRiskJob(DataStream<TradeSignal> tradeSignals,
            DataStream<ExecReport> execReports,
            DataStream<AccountPolicy> accountPolicies,
            Sink<Position> positionSink,
            Sink<Portfolio> portfolioSink,
            Sink<RiskAlert> riskAlertSink,
            TrackingSinkFactory trackingSinkFactory,
            Sink<TradeMatch> tradeMatchSink,
            Sink<RealizedPnl> realizedPnlSink,
            Sink<UnrealizedPnl> unrealizedPnlSink,
            Sink<PositionClose> positionCloseSink) {
        this.tradeSignals = tradeSignals;
        this.execReports = execReports;
        this.accountPolicies = accountPolicies;
        this.positionSink = positionSink;
        this.portfolioSink = portfolioSink;
        this.riskAlertSink = riskAlertSink;
        this.trackingSinkFactory = trackingSinkFactory;
        this.tradeMatchSink = tradeMatchSink;
        this.realizedPnlSink = realizedPnlSink;
        this.unrealizedPnlSink = unrealizedPnlSink;
        this.positionCloseSink = positionCloseSink;
    }

    public void run() throws Exception {
        DataStream<TradeSignal> acceptedOrders = tradeSignals
                .keyBy(ts -> ts.accountId)
                .connect(accountPolicies.keyBy(p -> p.accountId))
                .process(new PreTradeRiskCheckWithPolicy());

        DataStream<ExecReport> simulatedExecReports = acceptedOrders
                .map(new FakeFill());

        DataStream<ExecReport> allExecReports = execReports.union(simulatedExecReports);

        SingleOutputStreamOperator<Position> positions = allExecReports
                .keyBy(r -> r.accountId + "|" + r.symbol)
                .process(new PositionUpdater());

        // Side output: position closes
        DataStream<PositionClose> positionCloses = positions.getSideOutput(PositionUpdater.POSITION_CLOSE_TAG);

        // FIFO trade matching to compute realized P&L for closed portions
        DataStream<TradeMatch> tradeMatches = allExecReports
                .keyBy(r -> r.accountId + "|" + r.symbol)
                .process(new FIFOTradeMatchingEngine());

        // Aggregate realized PnL per account-symbol from trade matches
        DataStream<RealizedPnl> realizedPnl = tradeMatches
                .keyBy(m -> m.accountId + "|" + m.symbol)
                .process(new RealizedPnlAggregator());

        // Derive price ticks from execution reports (proxy for market prices)
        DataStream<PriceTick> priceTicks = allExecReports
                .map(er -> new PriceTick(er.accountId, er.symbol, er.fillPrice, er.ts))
                .returns(PriceTick.class);

        // Compute unrealized PnL for open positions using last seen price
        DataStream<UnrealizedPnl> unrealizedPnl = positions
                .keyBy(p -> p.accountId + "|" + p.symbol)
                .connect(priceTicks.keyBy(pt -> pt.accountId + "|" + pt.symbol))
                .process(new UnrealizedPnlCalculator());

        DataStream<Portfolio> portfolios = positions
                .keyBy(p -> p.accountId)
                .connect(accountPolicies.keyBy(p -> p.accountId))
                .process(new PortfolioUpdater());

        DataStream<RiskAlert> riskAlerts = portfolios
                .process(new RiskEngine());

        // Use sinks if provided, otherwise fall back to printing
        if (positionSink != null) {
            positions.sinkTo(positionSink);
        } else {
            positions.print("POS");
        }

        if (portfolioSink != null) {
            portfolios.sinkTo(portfolioSink);
        } else {
            portfolios.print("PF");
        }

        if (riskAlertSink != null) {
            riskAlerts.sinkTo(riskAlertSink);
        } else {
            riskAlerts.print("RISK");
        }

        if (positionCloseSink != null) {
            positionCloses.sinkTo(positionCloseSink);
        } else {
            positionCloses.print("POS_CLOSE");
        }

        // Optional sinks for trade history and P&L history
        if (tradeMatchSink != null) {
            tradeMatches.sinkTo(tradeMatchSink);
        }
        if (realizedPnlSink != null) {
            realizedPnl.sinkTo(realizedPnlSink);
        }
        if (unrealizedPnlSink != null) {
            unrealizedPnl.sinkTo(unrealizedPnlSink);
        }

        // Sink ExecReports to tracking system if enabled
        if (trackingSinkFactory != null) {
            allExecReports.sinkTo(trackingSinkFactory.createExecReportSink());
        }

        // Always print accepted orders, exec reports, and trade matches for debugging
        acceptedOrders.print("ACCEPTED");
        simulatedExecReports.print("EXEC");
        tradeMatches.print("MATCH");
        realizedPnl.print("REALIZED");
        unrealizedPnl.print("UNREAL");
    }

}
