import plotly.express as px
import pandas as pd

# Assuming you have the following data
data = {
    "Category": ["Valid", "Valid", "Valid", "Valid"],
    "Subcategory": ["Junk", "Junk", "No Junk", "No Junk"],
    "Status": ["Active", "Inactive", "Active", "Inactive"],
    "Count": [14865, 183, 6847177, 1332099]
}

df = pd.DataFrame(data)

# Creating a sunburst chart
fig = px.sunburst(
    df,
    path=['Category', 'Subcategory', 'Status'],  # Define the hierarchy
    values='Count',
    color='Category',  # Coloring based on the top-level category
    title="Interactive Sunburst Chart of Validity, Junk Status, and Active Status"
)

fig.show()
