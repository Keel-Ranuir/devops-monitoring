from influxdb import InfluxDBClient
from datetime import datetime

import pandas


def is_extremum(a, b, c):
    return (a < b > c) or (a > b < c)


chart = pandas.read_csv('chart.csv')

client = InfluxDBClient(host='localhost', port=8086)
client.create_database('users')
client.switch_database('users')

data = []
for index, row in chart.iterrows():
    data.append('{measurement} value={value} {timestamp}'.format(
        measurement='in_game',
        value=row["In-Game"],
        timestamp=int(datetime.fromisoformat(row["DateTime"]).timestamp())
    ))

client.write_points(data, time_precision='s', protocol='line')

data_array = chart.to_numpy()
critical_points = []

for i in range(1, data_array.shape[0] - 1):
    if is_extremum(data_array[i-1][3], data_array[i][3], data_array[i+1][3]):
        critical_points.append('{measurement} value={value} {timestamp}'.format(
            measurement='in_game_extremums',
            value=data_array[i][3],
            timestamp=int(datetime.fromisoformat(data_array[i][0]).timestamp())
        ))

print(data_array[:50])
print(critical_points)
print(len(critical_points))
client.write_points(critical_points, time_precision='s', protocol='line')
