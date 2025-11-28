import plotly.express as px

def bar_chart(df, x, y, title):
    fig = px.bar(df, x=x, y=y, title=title)
    return fig

def line_chart(df, x, y, title):
    fig = px.line(df, x=x, y=y, title=title)
    return fig