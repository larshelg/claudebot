package com.example.flink.jobs;

import com.alibaba.fluss.flink.sink.FlussSink;
import com.alibaba.fluss.flink.sink.serializer.RowDataSerializationSchema;
import com.alibaba.fluss.flink.source.FlussSource;
import com.alibaba.fluss.flink.source.enumerator.initializer.OffsetsInitializer;
import com.example.flink.domain.*;
import com.example.flink.tradeengine.PortfolioAndRiskJob;
import com.example.flink.tradeengine.fluss.Adapters.FlussRowMappers;
import com.example.flink.tradeengine.fluss.Adapters.PojoToFlussSink;
import com.example.flink.helpers.SerializableFunction;
import com.example.flink.tradeengine.fluss.Adapters.ExecReportDeserializationSchema;
import com.example.flink.tradeengine.fluss.Adapters.TradeSignalDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;

import java.time.Duration;

/**
 * TradingEngineJob wires PortfolioAndRiskJob to Fluss sources and sinks.
 *
 * Sources:
 * - Exec reports: fluss.exec_report_history (append)
 * - Trade signals: (optional) — default empty stream
 *
 * Sinks (configure via Fluss):
 * - Latest/upsert: open_positions_latest, portfolio_latest,
 * realized_pnl_latest, unrealized_pnl_latest
 * - History/append: exec_report_history, trade_match_history,
 * position_close_history, risk_alerts
 */
public class TradingEngineJob {

        public static void main(String[] args) throws Exception {
                String flussBootstrap = System.getProperty("fluss.bootstrap", "coordinator-server:9123");
                String flussDatabase = System.getProperty("fluss.database", "fluss");

                StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                env.setParallelism(1);

                // 1) Sources
                // ExecReports from Fluss (append-only history)
                var execReportSource = FlussSource.<ExecReport>builder()
                                .setBootstrapServers(flussBootstrap)
                                .setDatabase(flussDatabase)
                                .setTable("exec_report_history")
                                .setStartingOffsets(OffsetsInitializer.earliest())
                                .setDeserializationSchema(new ExecReportDeserializationSchema())
                                .build();

                DataStream<ExecReport> execReports = env.fromSource(
                                execReportSource,
                                WatermarkStrategy.<ExecReport>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                                .withTimestampAssigner((er, ts) -> er.ts),
                                "Fluss ExecReport Source");

                // TradeSignals from Fluss (append) - produced by StrategyChooserJob
                var tradeSignalSource = FlussSource.<TradeSignal>builder()
                                .setBootstrapServers(flussBootstrap)
                                .setDatabase(flussDatabase)
                                .setTable("trade_signal_history")
                                .setStartingOffsets(OffsetsInitializer.earliest())
                                .setDeserializationSchema(new TradeSignalDeserializationSchema())
                                .build();
                DataStream<TradeSignal> tradeSignals = env.fromSource(
                                tradeSignalSource,
                                WatermarkStrategy.<TradeSignal>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                                .withTimestampAssigner((t, ts) -> t.ts),
                                "Fluss TradeSignal Source");

                // 2) Account policies (simple mapping from signals; can be replaced with real
                // source)
                DataStream<AccountPolicy> accountPolicies = tradeSignals
                                .map(ts -> new AccountPolicy(ts.accountId, 3, "ACTIVE", 100_000.0, ts.ts))
                                .returns(AccountPolicy.class);

                // 3) Prepare sinks for PortfolioAndRiskJob static processor

                // 4) Configure Fluss sinks
                // Build Fluss RowData sinks
                FlussSink<RowData> positionLatestSink = FlussSink.<RowData>builder()
                                .setBootstrapServers(flussBootstrap)
                                .setDatabase(flussDatabase)
                                .setTable("open_positions_latest")
                                .setSerializationSchema(new RowDataSerializationSchema(false, false))
                                .build();

                FlussSink<RowData> portfolioLatestSink = FlussSink.<RowData>builder()
                                .setBootstrapServers(flussBootstrap)
                                .setDatabase(flussDatabase)
                                .setTable("portfolio_latest")
                                .setSerializationSchema(new RowDataSerializationSchema(false, false))
                                .build();

                FlussSink<RowData> realizedLatestSink = FlussSink.<RowData>builder()
                                .setBootstrapServers(flussBootstrap)
                                .setDatabase(flussDatabase)
                                .setTable("realized_pnl_latest")
                                .setSerializationSchema(new RowDataSerializationSchema(false, false))
                                .build();

                FlussSink<RowData> unrealizedLatestSink = FlussSink.<RowData>builder()
                                .setBootstrapServers(flussBootstrap)
                                .setDatabase(flussDatabase)
                                .setTable("unrealized_pnl_latest")
                                .setSerializationSchema(new RowDataSerializationSchema(false, false))
                                .build();

                FlussSink<RowData> tradeMatchHistorySink = FlussSink.<RowData>builder()
                                .setBootstrapServers(flussBootstrap)
                                .setDatabase(flussDatabase)
                                .setTable("trade_match_history")
                                .setSerializationSchema(new RowDataSerializationSchema(true, false))
                                .build();

                FlussSink<RowData> positionCloseHistorySink = FlussSink.<RowData>builder()
                                .setBootstrapServers(flussBootstrap)
                                .setDatabase(flussDatabase)
                                .setTable("position_close_history")
                                .setSerializationSchema(new RowDataSerializationSchema(true, false))
                                .build();

                FlussSink<RowData> riskAlertsHistorySink = FlussSink.<RowData>builder()
                                .setBootstrapServers(flussBootstrap)
                                .setDatabase(flussDatabase)
                                .setTable("risk_alerts")
                                .setSerializationSchema(new RowDataSerializationSchema(true, false))
                                .build();

                var sinks = new PortfolioAndRiskJob.Sinks(
                                new PojoToFlussSink<>(positionLatestSink, (SerializableFunction<Position, RowData>) FlussRowMappers::positionToRow),
                                new PojoToFlussSink<>(portfolioLatestSink, (SerializableFunction<Portfolio, RowData>) FlussRowMappers::portfolioToRow),
                                new PojoToFlussSink<>(riskAlertsHistorySink, (SerializableFunction<RiskAlert, RowData>) FlussRowMappers::riskAlertToRow),
                                new PojoToFlussSink<>(tradeMatchHistorySink, (SerializableFunction<TradeMatch, RowData>) FlussRowMappers::tradeMatchToRow),
                                new PojoToFlussSink<>(realizedLatestSink, (SerializableFunction<RealizedPnl, RowData>) FlussRowMappers::realizedToRow),
                                new PojoToFlussSink<>(unrealizedLatestSink, (SerializableFunction<UnrealizedPnl, RowData>) FlussRowMappers::unrealizedToRow),
                                new PojoToFlussSink<>(positionCloseHistorySink, (SerializableFunction<PositionClose, RowData>) FlussRowMappers::positionCloseToRow));

                // 5) Build and attach sinks via static processor
                PortfolioAndRiskJob.processExecReports(execReports, accountPolicies, sinks);
                env.execute("Trading Engine Job");
        }
}
