"""
Create interactive dashboards from Gold layer data using Plotly
Generates HTML files with visualizations for collision and claims analytics
"""
import sys
import os

# Add pip packages directory to path if it exists
pip_packages = "/workspace/.pip-packages"
if os.path.exists(pip_packages):
    sys.path.insert(0, pip_packages)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px
import pandas as pd

GOLD_BASE = "data/gold"
DASHBOARD_OUT = "dashboards"

def load_data(spark):
    """Load Gold layer tables into Spark DataFrames"""
    print("Loading Gold layer tables...")
    
    fact_collisions = spark.read.parquet(f"{GOLD_BASE}/fact_collisions")
    fact_claims = spark.read.parquet(f"{GOLD_BASE}/fact_claims")
    dim_time = spark.read.parquet(f"{GOLD_BASE}/dim_time")
    dim_location = spark.read.parquet(f"{GOLD_BASE}/dim_location")
    dim_vehicle = spark.read.parquet(f"{GOLD_BASE}/dim_vehicle")
    dim_factor = spark.read.parquet(f"{GOLD_BASE}/dim_factor")
    
    return {
        'fact_collisions': fact_collisions,
        'fact_claims': fact_claims,
        'dim_time': dim_time,
        'dim_location': dim_location,
        'dim_vehicle': dim_vehicle,
        'dim_factor': dim_factor
    }

def spark_to_pandas(spark_df, limit=None):
    """Convert Spark DataFrame to Pandas DataFrame"""
    if limit:
        return spark_df.limit(limit).toPandas()
    return spark_df.toPandas()

def create_collision_hotspots_dashboard(tables):
    """Create collision hotspots visualization"""
    print("\n=== Creating Collision Hotspots Dashboard ===")
    
    # Join collisions with location
    hotspots_df = (tables['fact_collisions']
                  .join(tables['dim_location'], "location_key")
                  .select("borough", "zip_code", "latitude", "longitude", 
                          "collision_id", "persons_injured", "persons_killed",
                          "severity_category")
                  .filter("latitude IS NOT NULL AND longitude IS NOT NULL")
                  .limit(10000))  # Limit for performance
    
    hotspots_pd = spark_to_pandas(hotspots_df)
    
    if len(hotspots_pd) == 0:
        # Create empty figure if no data
        fig = go.Figure()
        fig.add_annotation(text="No location data available", xref="paper", yref="paper", x=0.5, y=0.5)
        fig.update_layout(title="Collision Hotspots Map - NYC", height=600)
        return fig
    
    # Fill NaN values for size column
    hotspots_pd['persons_injured'] = hotspots_pd['persons_injured'].fillna(0)
    
    # Create scatter mapbox
    try:
        fig = px.scatter_mapbox(
            hotspots_pd,
            lat="latitude",
            lon="longitude",
            color="severity_category",
            size="persons_injured",
            hover_data=["borough", "zip_code", "persons_injured", "persons_killed"],
            color_discrete_map={
                "Fatal": "red",
                "Injury": "orange",
                "Property Damage Only": "blue"
            },
            zoom=10,
            height=600,
            title="Collision Hotspots Map - NYC"
        )
        
        fig.update_layout(
            mapbox_style="open-street-map",
            margin={"r": 0, "t": 50, "l": 0, "b": 0}
        )
    except Exception as e:
        # Fallback to regular scatter plot if mapbox fails
        fig = px.scatter(
            hotspots_pd,
            x="longitude",
            y="latitude",
            color="severity_category",
            size="persons_injured",
            hover_data=["borough", "zip_code", "persons_injured", "persons_killed"],
            title="Collision Hotspots Map - NYC"
        )
    
    return fig

def create_time_trends_dashboard(tables):
    """Create time-based trend visualizations"""
    print("\n=== Creating Time Trends Dashboard ===")
    
    # Monthly trends
    monthly_df = (tables['fact_collisions']
                 .join(tables['dim_time'], "time_key")
                 .groupBy("year", "month", "month_name")
                 .agg({
                     "collision_id": "count",
                     "persons_injured": "sum",
                     "persons_killed": "sum"
                 })
                 .withColumnRenamed("count(collision_id)", "collision_count")
                 .withColumnRenamed("sum(persons_injured)", "total_injured")
                 .withColumnRenamed("sum(persons_killed)", "total_killed")
                 .orderBy("year", "month"))
    
    monthly_pd = spark_to_pandas(monthly_df)
    monthly_pd['date'] = pd.to_datetime(monthly_pd[['year', 'month']].assign(day=1))
    
    # Hourly distribution
    hourly_df = (tables['fact_collisions']
                .join(tables['dim_time'], "time_key")
                .filter("hour IS NOT NULL")
                .groupBy("hour")
                .agg({
                    "collision_id": "count"
                })
                .withColumnRenamed("count(collision_id)", "collision_count")
                .orderBy("hour"))
    
    hourly_pd = spark_to_pandas(hourly_df)
    
    # Day of week distribution
    weekday_df = (tables['fact_collisions']
                 .join(tables['dim_time'], "time_key")
                 .filter("day_name IS NOT NULL")
                 .groupBy("day_name", "day_of_week")
                 .agg({
                     "collision_id": "count"
                 })
                 .withColumnRenamed("count(collision_id)", "collision_count")
                 .orderBy("day_of_week"))
    
    weekday_pd = spark_to_pandas(weekday_df)
    
    # Create subplots
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=('Monthly Collision Trends', 'Collisions by Hour of Day', 
                       'Collisions by Day of Week', 'Monthly Injury Trends'),
        specs=[[{"secondary_y": False}, {"secondary_y": False}],
               [{"secondary_y": False}, {"secondary_y": False}]]
    )
    
    # Monthly trends
    fig.add_trace(
        go.Scatter(x=monthly_pd['date'], y=monthly_pd['collision_count'],
                  mode='lines+markers', name='Collisions',
                  line=dict(color='blue', width=2)),
        row=1, col=1
    )
    
    # Hourly distribution
    fig.add_trace(
        go.Bar(x=hourly_pd['hour'], y=hourly_pd['collision_count'],
              name='Collisions by Hour', marker_color='orange'),
        row=1, col=2
    )
    
    # Day of week
    fig.add_trace(
        go.Bar(x=weekday_pd['day_name'], y=weekday_pd['collision_count'],
              name='Collisions by Day', marker_color='green'),
        row=2, col=1
    )
    
    # Monthly injuries
    fig.add_trace(
        go.Scatter(x=monthly_pd['date'], y=monthly_pd['total_injured'],
                  mode='lines+markers', name='Injuries',
                  line=dict(color='red', width=2)),
        row=2, col=2
    )
    
    fig.update_layout(
        height=800,
        title_text="Time-Based Collision Trends",
        showlegend=True
    )
    
    return fig

def create_claims_dashboard(tables):
    """Create claims analysis visualizations"""
    print("\n=== Creating Claims Dashboard ===")
    
    # Claims by borough
    borough_claims_df = (tables['fact_claims']
                        .join(tables['fact_collisions'], "collision_id")
                        .join(tables['dim_location'], "location_key")
                        .filter("borough IS NOT NULL")
                        .groupBy("borough")
                        .agg(
                            F.count("claim_id").alias("claim_count"),
                            F.sum("claim_amount").alias("total_amount"),
                            F.avg("claim_amount").alias("avg_amount")
                        )
                        .orderBy(F.col("total_amount").desc()))
    
    borough_pd = spark_to_pandas(borough_claims_df)
    
    # Claims by severity
    severity_claims_df = (tables['fact_claims']
                         .join(tables['fact_collisions'], "collision_id")
                         .groupBy("fact_collisions.severity_category")
                         .agg(
                             F.count("claim_id").alias("claim_count"),
                             F.sum("claim_amount").alias("total_amount"),
                             F.avg("claim_amount").alias("avg_amount")
                         )
                         .withColumnRenamed("fact_collisions.severity_category", "severity_category"))
    
    severity_pd = spark_to_pandas(severity_claims_df)
    
    # Claim status distribution
    status_df = (tables['fact_claims']
                .groupBy("claim_status")
                .agg(
                    F.count("claim_id").alias("claim_count"),
                    F.sum("claim_amount").alias("total_amount")
                ))
    
    status_pd = spark_to_pandas(status_df)
    
    # Days to close distribution (for closed claims)
    days_close_df = (tables['fact_claims']
                     .filter("claim_status = 'CLOSED' AND days_to_close IS NOT NULL")
                     .select("days_to_close")
                     .limit(10000))
    
    days_close_pd = spark_to_pandas(days_close_df)
    
    # Create subplots
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=('Total Claim Amount by Borough', 'Average Claim Amount by Severity',
                       'Claim Status Distribution', 'Days to Close Distribution'),
        specs=[[{"type": "bar"}, {"type": "bar"}],
               [{"type": "pie"}, {"type": "histogram"}]]
    )
    
    # Borough totals
    fig.add_trace(
        go.Bar(x=borough_pd['borough'], y=borough_pd['total_amount'],
              name='Total Amount', marker_color='steelblue'),
        row=1, col=1
    )
    
    # Severity averages
    fig.add_trace(
        go.Bar(x=severity_pd['severity_category'], y=severity_pd['avg_amount'],
              name='Avg Amount', marker_color='coral'),
        row=1, col=2
    )
    
    # Status pie chart
    fig.add_trace(
        go.Pie(labels=status_pd['claim_status'], values=status_pd['claim_count'],
              name='Status'),
        row=2, col=1
    )
    
    # Days to close histogram
    fig.add_trace(
        go.Histogram(x=days_close_pd['days_to_close'], nbinsx=30,
                    name='Days to Close', marker_color='lightgreen'),
        row=2, col=2
    )
    
    fig.update_layout(
        height=800,
        title_text="Claims Analysis Dashboard",
        showlegend=True
    )
    
    return fig

def create_severity_dashboard(tables):
    """Create severity analysis visualizations"""
    print("\n=== Creating Severity Dashboard ===")
    
    # Severity distribution
    severity_df = (tables['fact_collisions']
                   .groupBy("severity_category")
                   .agg({
                       "collision_id": "count"
                   })
                   .withColumnRenamed("count(collision_id)", "collision_count"))
    
    severity_pd = spark_to_pandas(severity_df)
    
    # Severity by borough
    severity_borough_df = (tables['fact_collisions']
                          .join(tables['dim_location'], "location_key")
                          .filter("borough IS NOT NULL")
                          .groupBy("borough", "severity_category")
                          .agg({
                              "collision_id": "count"
                          })
                          .withColumnRenamed("count(collision_id)", "collision_count"))
    
    severity_borough_pd = spark_to_pandas(severity_borough_df)
    
    # Create subplots
    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=('Severity Distribution', 'Severity by Borough'),
        specs=[[{"type": "pie"}, {"type": "bar"}]]
    )
    
    # Severity pie chart
    fig.add_trace(
        go.Pie(labels=severity_pd['severity_category'], values=severity_pd['collision_count'],
              name='Severity'),
        row=1, col=1
    )
    
    # Severity by borough (stacked bar)
    for severity in severity_borough_pd['severity_category'].unique():
        subset = severity_borough_pd[severity_borough_pd['severity_category'] == severity]
        fig.add_trace(
            go.Bar(x=subset['borough'], y=subset['collision_count'],
                  name=severity),
            row=1, col=2
        )
    
    fig.update_layout(
        height=500,
        title_text="Collision Severity Analysis",
        barmode='stack',
        showlegend=True
    )
    
    return fig

def create_contributing_factors_dashboard(tables):
    """Create contributing factors visualization"""
    print("\n=== Creating Contributing Factors Dashboard ===")
    
    # Top contributing factors - use primary_factor_key
    factors_df = (tables['fact_collisions']
                 .join(tables['dim_factor'], 
                       F.col("primary_factor_key") == F.col("factor_key"),
                       "left")
                 .filter("contributing_factor IS NOT NULL")
                 .groupBy("contributing_factor")
                 .agg(F.count("collision_id").alias("collision_count"))
                 .orderBy(F.col("collision_count").desc())
                 .limit(15))
    
    factors_pd = spark_to_pandas(factors_df)
    
    # Create horizontal bar chart
    fig = go.Figure()
    
    fig.add_trace(
        go.Bar(
            x=factors_pd['collision_count'],
            y=factors_pd['factor_description'],
            orientation='h',
            marker_color='indianred'
        )
    )
    
    fig.update_layout(
        title="Top 15 Contributing Factors",
        xaxis_title="Number of Collisions",
        yaxis_title="Contributing Factor",
        height=600
    )
    
    return fig

def create_summary_dashboard(tables):
    """Create summary statistics dashboard"""
    print("\n=== Creating Summary Dashboard ===")
    
    # Calculate key metrics
    total_collisions = tables['fact_collisions'].count()
    total_claims = tables['fact_claims'].count()
    
    total_claim_amount = (tables['fact_claims']
                         .agg({"claim_amount": "sum"})
                         .collect()[0][0]) or 0
    
    avg_claim_amount = (tables['fact_claims']
                       .agg({"claim_amount": "avg"})
                       .collect()[0][0]) or 0
    
    # Try to get injury/kill data - check if data exists
    # Note: If these show 0, the Silver layer may need regeneration
    injured_result = (tables['fact_collisions']
                     .agg(F.coalesce(F.sum("persons_injured"), F.lit(0)).alias("total"))
                     .collect()[0])
    total_injured = injured_result['total'] if injured_result['total'] is not None else 0
    
    killed_result = (tables['fact_collisions']
                    .agg(F.coalesce(F.sum("persons_killed"), F.lit(0)).alias("total"))
                    .collect()[0])
    total_killed = killed_result['total'] if killed_result['total'] is not None else 0
    
    # Create summary cards
    fig = go.Figure()
    
    metrics = [
        ['Total Collisions', f'{total_collisions:,}'],
        ['Total Claims', f'{total_claims:,}'],
        ['Total Claim Amount', f'${total_claim_amount:,.2f}'],
        ['Avg Claim Amount', f'${avg_claim_amount:,.2f}'],
        ['Total Injuries', f'{total_injured:,}'],
        ['Total Fatalities', f'{total_killed:,}']
    ]
    
    # Create table
    fig.add_trace(
        go.Table(
            header=dict(values=['Metric', 'Value'],
                       fill_color='paleturquoise',
                       align='left',
                       font=dict(size=14, color='black')),
            cells=dict(values=list(zip(*metrics)),
                     fill_color='lavender',
                     align='left',
                     font=dict(size=12))
        )
    )
    
    fig.update_layout(
        title="RoadSafe Analytics - Summary Statistics",
        height=400
    )
    
    return fig

def main():
    """Main function to generate all dashboards"""
    spark = SparkSession.builder.appName("roadsafe-dashboards").getOrCreate()
    
    # Create output directory
    os.makedirs(DASHBOARD_OUT, exist_ok=True)
    
    # Load data
    tables = load_data(spark)
    
    print("\n" + "="*80)
    print("GENERATING DASHBOARDS")
    print("="*80)
    
    # Generate dashboards
    dashboards = {
        '01_collision_hotspots.html': create_collision_hotspots_dashboard,
        '02_time_trends.html': create_time_trends_dashboard,
        '03_claims_analysis.html': create_claims_dashboard,
        '04_severity_analysis.html': create_severity_dashboard,
        '05_contributing_factors.html': create_contributing_factors_dashboard,
        '00_summary.html': create_summary_dashboard
    }
    
    for filename, dashboard_func in dashboards.items():
        try:
            print(f"\nGenerating {filename}...")
            fig = dashboard_func(tables)
            filepath = os.path.join(DASHBOARD_OUT, filename)
            fig.write_html(filepath)
            print(f"✅ Saved: {filepath}")
        except Exception as e:
            print(f"❌ Error generating {filename}: {str(e)}")
            import traceback
            traceback.print_exc()
    
    print("\n" + "="*80)
    print("DASHBOARD GENERATION COMPLETE")
    print("="*80)
    print(f"\nDashboards saved to: {DASHBOARD_OUT}/")
    print("\nTo view dashboards:")
    print("  1. Open the HTML files in your web browser")
    print("  2. Or run: python -m http.server 8000 (from project root)")
    print("     Then visit: http://localhost:8000/dashboards/")
    
    spark.stop()

if __name__ == "__main__":
    main()

