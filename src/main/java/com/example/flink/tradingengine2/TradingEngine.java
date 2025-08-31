package com.example.flink.tradingengine2;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.OutputTag;

import java.time.Duration;


public class TradingEngine {

    // Side outputs from PositionUpdater
    public static final OutputTag<PositionUpdater.PositionLotChange> LOTS_OUT =
            new OutputTag<>("lots-changelog") {};
    public static final OutputTag<PositionUpdater.PositionRollupChange> ROLLUP_OUT =
            new OutputTag<>("rollup-changelog") {};
    public static final OutputTag<PositionUpdater.RealizedPnlChange> PNL_OUT =
            new OutputTag<>("realizedpnl-latest") {};  // cumulative latest
    public static final OutputTag<PositionUpdater.TradeMatch> MATCH_OUT =
            new OutputTag<>("trade-match-history") {};

    public static void build(StreamExecutionEnvironment env,
                             DataStream<PositionUpdater.ExecReport> execReports,
                             Sink<PositionUpdater.PositionLotChange> positionLotChangeSink,
                             Sink<PositionUpdater.PositionRollupChange> positionRollupChangeSink) {
        build(env, execReports, positionLotChangeSink, positionRollupChangeSink, null);
    }

    // Overload with a third sink for realized PnL
    public static void build(StreamExecutionEnvironment env,
                             DataStream<PositionUpdater.ExecReport> execReports,
                             Sink<PositionUpdater.PositionLotChange> positionLotChangeSink,
                             Sink<PositionUpdater.PositionRollupChange> positionRollupChangeSink,
                             Sink<PositionUpdater.RealizedPnlChange> realizedPnlSink) {
        build(env, execReports, positionLotChangeSink, positionRollupChangeSink, null,null);
    }


        // keep existing overloads, and add one that accepts a TradeMatch sink
        public static void build(StreamExecutionEnvironment env,
                DataStream<PositionUpdater.ExecReport> execReports,
                Sink<PositionUpdater.PositionLotChange> positionLotChangeSink,
                Sink<PositionUpdater.PositionRollupChange> positionRollupChangeSink,
                Sink<PositionUpdater.RealizedPnlChange> realizedPnlSink,
                Sink<PositionUpdater.TradeMatch> tradeMatchSink) {

        DataStream<PositionUpdater.ExecReport> withWm = execReports
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<PositionUpdater.ExecReport>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((e, ts) -> e.ts)
                );

        SingleOutputStreamOperator<PositionUpdater.VoidOut> processed =
                withWm
                        .keyBy((KeySelector<PositionUpdater.ExecReport, PositionUpdater.PositionKey>) e ->
                                new PositionUpdater.PositionKey(e.accountId, e.strategyId, e.symbol))
                        .process(new PositionUpdater());  // emits to LOTS_OUT, ROLLUP_OUT, PNL_OUT

        DataStream<PositionUpdater.PositionLotChange> lotsUpdates = processed.getSideOutput(LOTS_OUT);
        DataStream<PositionUpdater.PositionRollupChange> rollupUpdates = processed.getSideOutput(ROLLUP_OUT);
        DataStream<PositionUpdater.RealizedPnlChange> pnlUpdates = processed.getSideOutput(PNL_OUT);
        DataStream<PositionUpdater.TradeMatch> matchUpdates       = processed.getSideOutput(MATCH_OUT);

        var tEnv = StreamTableEnvironment.create(env);
        FlussTableBridge.attach(tEnv, lotsUpdates, rollupUpdates, pnlUpdates);


        lotsUpdates.sinkTo(positionLotChangeSink);
        rollupUpdates.sinkTo(positionRollupChangeSink);
        if (realizedPnlSink != null) {
            pnlUpdates.sinkTo(realizedPnlSink);
        }
            if (tradeMatchSink != null)  matchUpdates.sinkTo(tradeMatchSink);


    }
}
