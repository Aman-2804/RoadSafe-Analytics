# Dashboards

This directory contains interactive HTML dashboards generated from Gold layer data using Plotly.

## Generated Dashboards

Run the visualization script to generate all dashboards:

```bash
# From Docker container
spark-submit --master spark://localhost:7077 spark/07_create_dashboards.py

# Or from host machine
docker compose exec spark bash -c "cd /workspace && spark-submit --master spark://localhost:7077 spark/07_create_dashboards.py"
```

### Available Dashboards

1. **00_summary.html** - Summary statistics and key metrics
   - Total collisions, claims, amounts
   - Injury and fatality counts
   - Quick overview of dataset

2. **01_collision_hotspots.html** - Interactive map of collision locations
   - Geographic visualization using OpenStreetMap
   - Color-coded by severity (Fatal/Injury/Property Damage)
   - Size indicates number of injuries
   - Hover for details (borough, zip, injuries, fatalities)

3. **02_time_trends.html** - Time-based analysis
   - Monthly collision trends over time
   - Hourly distribution (24-hour pattern)
   - Day of week patterns
   - Monthly injury trends

4. **03_claims_analysis.html** - Insurance claims insights
   - Total claim amount by borough
   - Average claim amount by severity
   - Claim status distribution (OPEN vs CLOSED)
   - Days to close distribution

5. **04_severity_analysis.html** - Collision severity breakdown
   - Severity distribution (pie chart)
   - Severity by borough (stacked bar chart)
   - Visual breakdown of Fatal/Injury/Property Damage

6. **05_contributing_factors.html** - Top contributing factors
   - Top 15 contributing factors ranked by frequency
   - Horizontal bar chart for easy reading
   - Identifies most common causes

## Viewing Dashboards

### Method 1: Direct File Opening
Simply open any HTML file in your web browser:
```bash
open dashboards/00_summary.html  # macOS
xdg-open dashboards/00_summary.html  # Linux
start dashboards/00_summary.html  # Windows
```

### Method 2: Local Web Server
Start a simple HTTP server:
```bash
# From project root
python -m http.server 8000

# Then visit in browser:
# http://localhost:8000/dashboards/00_summary.html
```

### Method 3: Copy to Host Machine
If running in Docker, copy dashboards to host:
```bash
docker cp roadsafe-spark:/workspace/dashboards ./dashboards
```

## Dashboard Features

- **Interactive**: Zoom, pan, hover for details
- **Exportable**: Right-click to save as PNG
- **Responsive**: Works on desktop and mobile browsers
- **No Dependencies**: Self-contained HTML files (includes Plotly.js)

## Technical Details

- **Library**: Plotly (Python + JavaScript)
- **Data Source**: Gold layer Parquet files
- **Format**: Standalone HTML files
- **Size**: Optimized for performance (samples large datasets)

## Customization

Edit `spark/07_create_dashboards.py` to:
- Add new visualizations
- Change color schemes
- Modify chart types
- Adjust data sampling limits
- Add filters or drill-downs

## Integration with BI Tools

The Gold layer tables can also be loaded into:
- **Tableau**: Connect to Parquet files
- **Power BI**: Import from Parquet
- **Apache Superset**: Create custom dashboards
- **Jupyter Notebooks**: Use Plotly directly

## Future Enhancements

- Real-time dashboard updates
- Filtering and drill-down capabilities
- Export to PDF functionality
- Scheduled dashboard generation
- Integration with web framework (Flask/Dash)



