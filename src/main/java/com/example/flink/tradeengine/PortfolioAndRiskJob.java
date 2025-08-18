package com.example.flink.tradeengine;

import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;

import com.example.flink.domain.AccountPolicy;
import com.example.flink.domain.ExecReport;
import com.example.flink.domain.Portfolio;
import com.example.flink.domain.Position;
import com.example.flink.domain.RiskAlert;
import com.example.flink.domain.TradeSignal;

public class PortfolioAndRiskJob implements Serializable {
    private final DataStream<TradeSignal> tradeSignals;
    private final DataStream<ExecReport> execReports;
    private final DataStream<AccountPolicy> accountPolicies;
    private final Sink<Position> positionSink;
    private final Sink<Portfolio> portfolioSink;
    private final Sink<RiskAlert> riskAlertSink;

    public PortfolioAndRiskJob(DataStream<TradeSignal> tradeSignals, DataStream<ExecReport> execReports) {
        this.tradeSignals = tradeSignals;
        this.execReports = execReports;
        this.accountPolicies = tradeSignals
                .map(ts -> new AccountPolicy(ts.accountId, 3, "ACTIVE", 100_000.0, ts.ts))
                .returns(AccountPolicy.class);
        this.positionSink = null;
        this.portfolioSink = null;
        this.riskAlertSink = null;
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
    }

    public void run() throws Exception {
        DataStream<TradeSignal> acceptedOrders = tradeSignals
                .keyBy(ts -> ts.accountId)
                .connect(accountPolicies.keyBy(p -> p.accountId))
                .process(new PreTradeRiskCheckWithPolicy());

        DataStream<ExecReport> simulatedExecReports = acceptedOrders
                .map(new FakeFill());

        DataStream<ExecReport> allExecReports = execReports.union(simulatedExecReports);

        DataStream<Position> positions = allExecReports
                .keyBy(r -> r.accountId + "|" + r.symbol)
                .process(new PositionUpdater());

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
        
        // Always print accepted orders and exec reports for debugging
        acceptedOrders.print("ACCEPTED");
        simulatedExecReports.print("EXEC");
    }

}
