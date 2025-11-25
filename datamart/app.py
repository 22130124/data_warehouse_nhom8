from flask import Flask, render_template_string
import mysql.connector
import pandas as pd
import plotly.express as px
import plotly.io as pio

app = Flask(__name__)

# Template HTML nhúng tất cả biểu đồ
HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Data Mart Dashboard</title>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
</head>
<body>
    <h1>Data Mart Dashboard</h1>
    {% for title, plot in plots %}
        <h2>{{ title }}</h2>
        <div>{{ plot|safe }}</div>
        <hr>
    {% endfor %}
</body>
</html>
"""

# Các bảng aggregate cần hiển thị
AGG_TABLES = [
    {"name": "agg_job_by_company", "group_col": "company_name", "value_col": "total_jobs", "title": "Jobs by Company"},
    {"name": "agg_job_by_location", "group_col": "location", "value_col": "total_jobs", "title": "Jobs by Location"},
    {"name": "agg_job_by_salary", "group_col": "salary", "value_col": "total_jobs", "title": "Jobs by Salary"},
    {"name": "agg_job_by_experience", "group_col": "experience_required", "value_col": "total_jobs", "title": "Jobs by Experience"}
]

@app.route("/")
def dashboard():
    # Kết nối Data Mart
    conn = mysql.connector.connect(
        host="localhost",
        user="khanh",
        password="khanh123!",
        database="db_datamart"
    )

    plots = []

    for table in AGG_TABLES:
        try:
            query = f"SELECT {table['group_col']}, {table['value_col']} FROM {table['name']}"
            df = pd.read_sql(query, conn)

            # Nếu bảng trống, bỏ qua
            if df.empty:
                continue

            # Vẽ biểu đồ
            fig = px.bar(df, x=table['group_col'], y=table['value_col'], title=table['title'])
            plot_div = pio.to_html(fig, full_html=False)
            plots.append((table['title'], plot_div))

        except Exception as e:
            print(f"Error loading {table['name']}: {e}")

    conn.close()
    return render_template_string(HTML_TEMPLATE, plots=plots)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)