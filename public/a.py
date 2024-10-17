import pandas as pd

# Example DataFrame
data = {
    'Validity': ['Valid', 'Valid', 'Valid', 'Valid', 'Valid', 'Valid'],
    'Junk Data': ['Junk', 'No Junk', 'Junk', 'Junk', 'No Junk', 'No Junk'],
    'Active Status': [None, None, 'Active', 'Inactive', 'Active', 'Inactive'],
    'Count': [14685, 684717, 14685, 1833, 684717, 133021]
}

df = pd.DataFrame(data)

import plotly.express as px

fig = px.sunburst(df, path=['Validity', 'Junk Data', 'Active Status'], values='Count',
                  title='Interactive Hierarchical Pie Chart')
fig.update_traces(textinfo='label+percent entry')
fig.show()
import dash
from dash import html, dcc, Input, Output
import plotly.graph_objects as go

# Create a Dash application
app = dash.Dash(__name__)

# Layout
app.layout = html.Div([
    dcc.Graph(id='pie-chart'),
    html.P("Click on a section to drill-down."),
])

# Callback to update Pie Chart based on Clicks
@app.callback(
    Output('pie-chart', 'figure'),
    [Input('pie-chart', 'clickData')],
    prevent_initial_call=True
)
def display_click_data(clickData):
    if clickData:
        label = clickData['points'][0]['label']
        filtered_df = df[df['Junk Data'] == label] if label in ['Junk', 'No Junk'] else df
        fig = px.sunburst(filtered_df, path=['Validity', 'Junk Data', 'Active Status'], values='Count')
        fig.update_traces(textinfo='label+percent entry')
        return fig
    else:
        return px.sunburst(df, path=['Validity', 'Junk Data', 'Active Status'], values='Count',
                           title='Interactive Hierarchical Pie Chart')

# Run the app
if __name__ == '__main__':
    app.run_server(debug=True)
