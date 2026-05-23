package tpch.operator;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import tpch.pojo.line_item;
import tpch.pojo.order;

public class join_order_line extends KeyedCoProcessFunction<String, line_item, order, String> {

    private ValueState<order> order_state;

    @Override
    public void open(Configuration parameters) throws Exception {
        order_state = getRuntimeContext().getState(
                new ValueStateDescriptor<>("order_state", order.class)
        );
    }

    @Override
    public void processElement1(line_item lineItem, Context ctx, Collector<String> out) throws Exception {
        order o = order_state.value();
        if (o != null) {
            out.collect(o.cust_key + "|" + lineItem.revenue);
        }
    }

    @Override
    public void processElement2(order o, Context ctx, Collector<String> out) throws Exception {
        order_state.update(o);
    }
}