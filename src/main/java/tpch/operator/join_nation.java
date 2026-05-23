package tpch.operator;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import tpch.pojo.nation;

public class join_nation extends KeyedCoProcessFunction<String, String, nation, String> {

    private ValueState<nation> nation_state;

    @Override
    public void open(Configuration parameters) throws Exception {
        nation_state = getRuntimeContext().getState(
                new ValueStateDescriptor<>("nation_state", nation.class)
        );
    }

    @Override
    public void processElement1(String value, Context ctx, Collector<String> out) throws Exception {
        nation n = nation_state.value();
        if (n != null) {
            String[] parts = value.split("\\|");
            out.collect(parts[0] + "|" + parts[1] + "|" + parts[2] + "|" + n.name);
        }
    }

    @Override
    public void processElement2(nation n, Context ctx, Collector<String> out) throws Exception {
        nation_state.update(n);
    }
}