package tpch.pojo;

public class line_item {
    public String order_key;
    public double revenue;
    public String return_flag;

    public line_item() {}

    public line_item(String order_key, double revenue, String return_flag) {
        this.order_key = order_key;
        this.revenue = revenue;
        this.return_flag = return_flag;
    }
}