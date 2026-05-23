package tpch.main;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import tpch.pojo.*;
import tpch.operator.*;

import java.util.*;

public class tpch_q10_job {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String data_path = "/home/yinfangyuan/Cquirrel-TPC-H-Q10/src/main/java/tpch/data/";

        // 1. lineitem：只取退货 R
        DataStream<line_item> line_item_stream = env.readTextFile(data_path + "lineitem.tbl")
                .filter(line -> !line.trim().isEmpty())
                .map(line -> {
                    String[] parts = line.split("\\|");
                    String order_key = parts[0].trim();
                    double revenue = Double.parseDouble(parts[5]) * (1 - Double.parseDouble(parts[6]));
                    String return_flag = parts[8].trim();
                    return new line_item(order_key, revenue, return_flag);
                })
                .filter(li -> "R".equals(li.return_flag));

        // 2. orders：严格按题目要求，取 1993 年第四季度
        DataStream<order> order_stream = env.readTextFile(data_path + "orders.tbl")
                .filter(line -> !line.trim().isEmpty())
                .map(line -> {
                    String[] parts = line.split("\\|");
                    return new order(parts[0], parts[1], parts[4]);
                })
                .filter(o -> o.order_date.compareTo("1993-10-01") >= 0
                        && o.order_date.compareTo("1993-12-31") <= 0);

        // 3. customer
        DataStream<customer> customer_stream = env.readTextFile(data_path + "customer.tbl")
                .filter(line -> !line.trim().isEmpty())
                .map(line -> {
                    String[] parts = line.split("\\|");
                    return new customer(parts[0], parts[1], parts[3], Double.parseDouble(parts[5]));
                });

        // 4. nation
        DataStream<nation> nation_stream = env.readTextFile(data_path + "nation.tbl")
                .filter(line -> !line.trim().isEmpty())
                .map(line -> {
                    String[] parts = line.split("\\|");
                    return new nation(parts[0], parts[1]);
                });

        // 算子链路
        DataStream<String> join1 = line_item_stream.keyBy(li -> li.order_key)
                .connect(order_stream.keyBy(o -> o.order_key))
                .process(new join_order_line());

        DataStream<String> agg = join1.keyBy(s -> s.split("\\|")[0])
                .process(new agg_revenue());

        DataStream<String> join2 = agg.keyBy(s -> s.split("\\|")[0])
                .connect(customer_stream.keyBy(c -> c.cust_key))
                .process(new join_customer());

        DataStream<String> fullResult = join2.keyBy(s -> s.split("\\|")[3])
                .connect(nation_stream.keyBy(n -> n.nation_key))
                .process(new join_nation());

        // 关键：用 Sink 收集数据，做客户级汇总 + 排序 + 取前20
        fullResult.addSink(new RichSinkFunction<String>() {
            private final List<String> buffer = new ArrayList<>();

            @Override
            public void invoke(String value, Context context) {
                buffer.add(value);
            }

            @Override
            public void close() {
                // 1. 按客户汇总退货总额
                Map<String, Double> customerTotal = new HashMap<>();
                Map<String, String> customerInfo = new HashMap<>();

                for (String line : buffer) {
                    String[] parts = line.split("\\|");
                    String custKey = parts[0];
                    double revenue = Double.parseDouble(parts[1]);
                    customerTotal.put(custKey, customerTotal.getOrDefault(custKey, 0.0) + revenue);
                    customerInfo.putIfAbsent(custKey, parts[2] + "|" + parts[3]);
                }

                // 2. 按退货总额降序排序
                List<Map.Entry<String, Double>> sortedList = new ArrayList<>(customerTotal.entrySet());
                sortedList.sort((a, b) -> Double.compare(b.getValue(), a.getValue()));

                // 3. 只保留前20个客户
                int limit = Math.min(20, sortedList.size());
                List<String> top20 = new ArrayList<>();
                for (int i = 0; i < limit; i++) {
                    Map.Entry<String, Double> entry = sortedList.get(i);
                    String custKey = entry.getKey();
                    String info = customerInfo.get(custKey);
                    top20.add(custKey + "|" + entry.getValue() + "|" + info);
                }

                // 4. 输出到 result.txt
                try (java.io.PrintWriter writer = new java.io.PrintWriter("/home/yinfangyuan/Cquirrel-TPC-H-Q10/result.txt")) {
                    for (String line : top20) {
                        writer.println(line);
                        System.out.println(line);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        env.execute("Cquirrel-TPC-H-Q10-Top20");
    }
}