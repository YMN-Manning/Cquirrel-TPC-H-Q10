import pandas as pd
import matplotlib.pyplot as plt
import matplotlib

# 设置中文字体
matplotlib.rcParams['font.sans-serif'] = ['SimHei', 'DejaVu Sans']
matplotlib.rcParams['axes.unicode_minus'] = False

# 读取结果文件
df = pd.read_csv(
    "/home/yinfangyuan/Cquirrel-TPC-H-Q10/result.txt",
    sep="|",
    names=["cust_key", "revenue", "cust_name", "nation_name"],
    header=None
)

# ====================== 取 TOP20 ======================
top20_customers = df.sort_values("revenue", ascending=False).head(20)

print("=" * 60)
print("          退货金额 TOP 20 客户")
print("=" * 60)
print(top20_customers.to_string(index=False))
print("=" * 60)

# ====================== 输出 CSV 文件 ======================
top20_customers.to_csv(
    "/home/yinfangyuan/Cquirrel-TPC-H-Q10/result.csv",
    index=False,
    encoding="utf-8-sig"
)

# 绘图
plt.figure(figsize=(12, 6))
bars = plt.bar(
    top20_customers["cust_name"],
    top20_customers["revenue"],
    color='#FF6B6B'
)

plt.xticks(rotation=45, ha='right', fontsize=10)
plt.title("1993年第四季度 客户退货金额排名 TOP20", fontsize=14, pad=20)
plt.xlabel("客户名称", fontsize=12)
plt.ylabel("退货金额", fontsize=12)
plt.tight_layout()

plt.savefig("/home/yinfangyuan/Cquirrel-TPC-H-Q10/top20_customers.png", dpi=300)
plt.close()

print("✅ TOP20 图表已生成：top20_customers.png")
print("✅ CSV 结果已输出：result.csv")