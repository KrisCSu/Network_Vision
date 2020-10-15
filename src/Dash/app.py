import os
import psycopg2
import socket
import pandas as pd
import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd



db_user = os.environ.get('DB_USER')
db_password = os.environ.get('DB_PASSWORD')


def get_data():
    try:
        connection = psycopg2.connect(
                       host = 'ec2-3-14-149-111.us-east-2.compute.amazonaws.com',
                       user = db_user,
                       password = db_password,
                       database = 'network_db',
                       port = '5431' )

        cursor = connection.cursor()
   # query = 'select "Protocol", count("Protocol") as "freq"'
   # query += 'from network group by "Protocol" order by "freq" desc limit 10;'
        query = 'SELECT * FROM network ORDER BY seq ASC LIMIT(5000)'
        cursor.execute(query)

        df = pd.DataFrame(cursor.fetchall())

        df.columns = ['Sequence', 'Time', 'Source', 'Destination', 'Protocol', 'Length', 'Info', 'Session_ID']
        return df

    except (Exception, psycopg2.Error) as error:
        print("Error fetching data from PostgreSQL table", error)

    finally:
        # closing database connection
         if (connection):
             cursor.close()
             connection.close()
             print("PostgreSQL connection is closed \n")


def make_plots(df):
    fig = make_subplots(rows = 3, cols = 1, specs=[[{"type": "table"}], [{"type": "pie"}], [{"type": "scatter"}]])

    fig.add_trace(go.Scatter(x=df['Time'], y=df['Length'], mode='lines'),row=3, col=1)


    #counts = df[4].value_counts().to_dict()
    #vals = list(counts.values())
    #label = list(counts.keys())
    #fig.add_trace(go.Pie(values=vals, labels= label, row=2, col=1))

    fig.add_trace(
        go.Table(
            header=dict(
                values=['seq', 'time', 'source', 'destination', 'protocol', 'length', 'info'],
                font=dict(size=10),
                align='left'
            ),
            cells=dict(
                values=[df[k].tolist() for k in df.columns[0:]],
                align='left')
        ),
       row=1,col=1)

    fig.update_layout(
        height=800,
        showlegend = False)
    return fig

data = get_data()
fig = make_plots(data)

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app.layout = html.Div(children=[
    html.H1(children='Network Vision'),

    html.Div(children='''
    '''),

    dcc.Graph(
        id='example-graph',
        figure=fig
    )
])
server = app.server

if __name__ == '__main__':
    #host = socket.gethostbyname(socket.gethostname())
    app.run_server(debug=True, host='0.0.0.0')