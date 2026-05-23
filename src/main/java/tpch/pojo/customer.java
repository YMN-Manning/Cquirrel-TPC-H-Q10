package tpch.pojo;

public class customer {
    public String cust_key;
    public String name;
    public String nation_key;
    public double acct_bal;

    public customer() {}

    public customer(String cust_key, String name, String nation_key, double acct_bal) {
        this.cust_key = cust_key;
        this.name = name;
        this.nation_key = nation_key;
        this.acct_bal = acct_bal;
    }
}