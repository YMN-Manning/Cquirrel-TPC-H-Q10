package tpch.operator;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import tpch.pojo.customer;

public class join_customer extends KeyedCoProcessFunction<String, String, customer, String> {

    private ValueState<customer> customer_state;

    @Override
    public void open(Configuration parameters) throws Exception {
        customer_state = getRuntimeContext().getState(
                new ValueStateDescriptor<>("customer_state", customer.class)
        );
    }

    @Override
    public void processElement1(String value, Context ctx, Collector<String> out) throws Exception {
        customer c = customer_state.value();
        if (c != null) {
            String[] parts = value.split("\\|");
            out.collect(parts[0] + "|" + parts[1] + "|" + c.name + "|" + c.nation_key);
        }
    }

    @Override
    public void processElement2(customer c, Context ctx, Collector<String> out) throws Exception {
        customer_state.update(c);
    }
}