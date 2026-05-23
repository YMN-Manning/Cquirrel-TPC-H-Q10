package tpch.operator;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class agg_revenue extends KeyedProcessFunction<String, String, String> {

    private ValueState<Double> sum_state;

    @Override
    public void open(Configuration parameters) throws Exception {
        sum_state = getRuntimeContext().getState(
                new ValueStateDescriptor<>("sum_revenue", Double.class)
        );
    }

    @Override
    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
        String[] parts = value.split("\\|");
        String cust_key = parts[0];
        double revenue = Double.parseDouble(parts[1]);

        Double sum = sum_state.value();
        if (sum == null) {
            sum = 0.0;
        }
        sum += revenue;
        sum_state.update(sum);

        out.collect(cust_key + "|" + sum);
    }
}