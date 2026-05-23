package tpch.pojo;

public class order {
    public String order_key;
    public String cust_key;
    public String order_date;

    public order() {}

    public order(String order_key, String cust_key, String order_date) {
        this.order_key = order_key;
        this.cust_key = cust_key;
        this.order_date = order_date;
    }
}