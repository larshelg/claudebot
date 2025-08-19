package com.example.flink.tradeengine;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import com.example.flink.domain.ExecReport;
import com.example.flink.domain.TradeMatch;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * FIFO Trade Matching Engine for P&L calculation.
 * 
 * Maintains a queue of unmatched buy orders per account-symbol key.
 * When a sell order arrives, matches it against the oldest buy orders (FIFO).
 * Generates TradeMatch records with realized P&L for each match.
 */
public class FIFOTradeMatchingEngine extends KeyedProcessFunction<String, ExecReport, TradeMatch> {
    
    // State: Queue of unmatched buy executions
    private ListState<ExecReport> unmatchedBuys;
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        
        // Initialize state for storing unmatched buy orders
        ListStateDescriptor<ExecReport> unmatchedBuysDescriptor = 
            new ListStateDescriptor<>("unmatchedBuys", Types.POJO(ExecReport.class));
        unmatchedBuys = getRuntimeContext().getListState(unmatchedBuysDescriptor);
    }
    
    @Override
    public void processElement(ExecReport exec, Context ctx, Collector<TradeMatch> out) throws Exception {
        if (exec.fillQty > 0) {
            // Buy order - add to unmatched buys queue
            unmatchedBuys.add(exec);
        } else if (exec.fillQty < 0) {
            // Sell order - match against oldest buys using FIFO
            matchSellAgainstBuys(exec, out);
        }
        // Ignore zero quantity executions
    }
    
    /**
     * Matches a sell execution against unmatched buy executions using FIFO.
     * Handles partial fills by matching against multiple buy orders if needed.
     */
    private void matchSellAgainstBuys(ExecReport sell, Collector<TradeMatch> out) throws Exception {
        double remainingSellQty = Math.abs(sell.fillQty); // Make positive for easier calculation
        List<ExecReport> updatedBuys = new ArrayList<>();
        
        // Process existing unmatched buys in FIFO order
        for (ExecReport buy : unmatchedBuys.get()) {
            if (remainingSellQty <= 0) {
                // Sell quantity fully matched, keep remaining buys
                updatedBuys.add(buy);
                continue;
            }
            
            double availableBuyQty = buy.fillQty;
            double matchedQty = Math.min(remainingSellQty, availableBuyQty);
            
            // Create trade match record
            TradeMatch match = new TradeMatch(
                sell.accountId,
                sell.symbol,
                buy.orderId,
                sell.orderId,
                matchedQty,
                buy.fillPrice,
                sell.fillPrice,
                buy.ts,
                sell.ts
            );
            
            out.collect(match);
            
            // Update quantities
            remainingSellQty -= matchedQty;
            availableBuyQty -= matchedQty;
            
            // If buy order still has remaining quantity, keep it (with reduced quantity)
            if (availableBuyQty > 0) {
                ExecReport partialBuy = new ExecReport(
                    buy.accountId,
                    buy.orderId,
                    buy.symbol,
                    availableBuyQty,
                    buy.fillPrice,
                    buy.status,
                    buy.ts
                );
                updatedBuys.add(partialBuy);
            }
        }
        
        // Update state with remaining unmatched buys
        unmatchedBuys.clear();
        for (ExecReport remainingBuy : updatedBuys) {
            unmatchedBuys.add(remainingBuy);
        }
        
        // If there's still unmatched sell quantity, it means we don't have enough buy orders
        // In a real system, this might be logged as a warning or handled differently
        if (remainingSellQty > 0) {
            // For now, we ignore unmatched sell quantity
            // In production, this might create an "unmatched sell" record or alert
        }
    }
}