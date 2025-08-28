package com.example.flink.strategyengine;

import com.example.flink.StrategySignal;
import com.example.flink.domain.AccountPolicy;
import com.example.flink.domain.TradeSignal;
import com.example.flink.domain.Position;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.io.Serializable;

public class StrategyChooserJob implements Serializable {
    private final DataStream<StrategySignal> strategySignals;
    private final DataStream<AccountPolicy> accountPolicies;
    private final Sink<TradeSignal> tradeSignalSink;
    private final DataStream<Position> positions;
    private DataStream<TradeSignal> acceptedOut;

    public StrategyChooserJob(
            DataStream<StrategySignal> strategySignals,
            DataStream<AccountPolicy> accountPolicies,
            DataStream<Position> positions,
            Sink<TradeSignal> tradeSignalSink) {
        this.strategySignals = strategySignals;
        this.accountPolicies = accountPolicies;
        this.positions = positions;
        this.tradeSignalSink = tradeSignalSink;
    }

    public void run() throws Exception {
        Results res = processStrategySignals(this.strategySignals, this.accountPolicies, this.positions,
                this.tradeSignalSink == null ? null : new Sinks(this.tradeSignalSink));
        this.acceptedOut = res.accepted;
    }

    public DataStream<TradeSignal> getAcceptedTradeSignals() {
        return acceptedOut;
    }

    public static class Results {
        public final DataStream<TradeSignal> accepted;

        public Results(DataStream<TradeSignal> accepted) {
            this.accepted = accepted;
        }
    }

    public static class Sinks {
        public final Sink<TradeSignal> tradeSignalSink;

        public Sinks(Sink<TradeSignal> tradeSignalSink) {
            this.tradeSignalSink = tradeSignalSink;
        }
    }

    public static Results processStrategySignals(
            DataStream<StrategySignal> strategySignals,
            DataStream<AccountPolicy> accountPolicies,
            DataStream<Position> positions,
            Sinks sinksOrNull) {
        DataStream<TradeSignal> rawTradeSignals = strategySignals
                .map(sig -> new TradeSignal(
                        sig.runId != null ? sig.runId : "ACC_DEFAULT",
                        sig.symbol,
                        "BUY".equalsIgnoreCase(sig.signal) ? 1.0
                                : ("SELL".equalsIgnoreCase(sig.signal) ? -1.0 : 0.0),
                        sig.close,
                        sig.timestamp))
                .returns(TradeSignal.class);

        DataStream<PreTradeRiskCheckWithPositions.Control> control = accountPolicies
                .map(PreTradeRiskCheckWithPositions.Control::fromPolicy)
                .returns(PreTradeRiskCheckWithPositions.Control.class)
                .union(positions
                        .map(PreTradeRiskCheckWithPositions.Control::fromPosition)
                        .returns(PreTradeRiskCheckWithPositions.Control.class));

        DataStream<TradeSignal> accepted = rawTradeSignals
                .keyBy(ts -> ts.accountId)
                .connect(control.keyBy(c -> c.accountId))
                .process(new PreTradeRiskCheckWithPositions());

        if (sinksOrNull != null && sinksOrNull.tradeSignalSink != null) {
            accepted.sinkTo(sinksOrNull.tradeSignalSink);
        } else {
            accepted.print("TRADE_SIGNALS");
        }
        return new Results(accepted);
    }
}
