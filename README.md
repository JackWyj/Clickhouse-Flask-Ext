Example
-----------


```python
from flask import Flask, jsonify
from clickhouse import ClickHouseDriver

app = Flask(__name__)

app.config['CLICKHOUSE_HOST'] = 'localhost'
app.config['CLICKHOUSE_PORT'] = 9000
app.config['CLICKHOUSE_DATABASE'] = 'default'
app.config['CLICKHOUSE_USERNAME'] = 'password'
app.config['CLICKHOUSE_PASSWORD'] = 'default'
app.config['CLICKHOUSE_CLIENT_NAME'] = 'clickhouse_driver'

db = ClickHouseDriver(app)


@app.route('/')
def show_tables():
    conn = db.connection
    data = conn.execute("show tables")
    return jsonify({'data': data})


if __name__ == "__main__":
    app.run()
    exit(0)

```