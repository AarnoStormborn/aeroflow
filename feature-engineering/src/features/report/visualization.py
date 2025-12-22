"""
Data visualization utilities for flight analysis.

Creates plots for:
- Geographic distribution
- Time series patterns
- Altitude/speed distributions
"""

from datetime import date
from pathlib import Path

import matplotlib.pyplot as plt
import seaborn as sns
import polars as pl
from loguru import logger


# Set style
sns.set_theme(style="darkgrid")
plt.rcParams["figure.figsize"] = (12, 8)
plt.rcParams["font.size"] = 10


def plot_geographic_distribution(
    df: pl.DataFrame,
    save_path: str | None = None,
    title: str = "Flight Distribution",
) -> None:
    """
    Plot geographic distribution of flights.
    
    Args:
        df: Flight data with latitude/longitude
        save_path: Optional path to save the plot
        title: Plot title
    """
    if df.is_empty():
        logger.warning("Empty DataFrame, skipping plot")
        return
    
    fig, ax = plt.subplots(figsize=(14, 10))
    
    # Scatter plot of positions
    scatter = ax.scatter(
        df["longitude"].to_numpy(),
        df["latitude"].to_numpy(),
        c=df["baro_altitude"].to_numpy() if "baro_altitude" in df.columns else "blue",
        cmap="viridis",
        alpha=0.5,
        s=10,
    )
    
    if "baro_altitude" in df.columns:
        plt.colorbar(scatter, label="Altitude (m)")
    
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.set_title(title)
    
    plt.tight_layout()
    
    if save_path:
        plt.savefig(save_path, dpi=150)
        logger.info(f"Saved plot to {save_path}")
    else:
        plt.show()
    
    plt.close()


def plot_hourly_traffic(
    df: pl.DataFrame,
    save_path: str | None = None,
    title: str = "Hourly Traffic Pattern",
) -> None:
    """
    Plot hourly traffic volume.
    
    Args:
        df: Flight data with 'hour' column
        save_path: Optional path to save the plot
        title: Plot title
    """
    if df.is_empty() or "hour" not in df.columns:
        logger.warning("Missing 'hour' column, skipping plot")
        return
    
    # Aggregate by hour
    hourly = df.group_by("hour").agg([
        pl.count().alias("record_count"),
        pl.col("icao24").n_unique().alias("unique_aircraft"),
    ]).sort("hour")
    
    fig, ax1 = plt.subplots(figsize=(12, 6))
    
    # Bar chart for record count
    ax1.bar(
        hourly["hour"].to_numpy(),
        hourly["record_count"].to_numpy(),
        alpha=0.7,
        label="Total Records",
        color="steelblue",
    )
    ax1.set_xlabel("Hour of Day")
    ax1.set_ylabel("Record Count", color="steelblue")
    ax1.tick_params(axis="y", labelcolor="steelblue")
    
    # Line for unique aircraft
    ax2 = ax1.twinx()
    ax2.plot(
        hourly["hour"].to_numpy(),
        hourly["unique_aircraft"].to_numpy(),
        color="coral",
        linewidth=2,
        marker="o",
        label="Unique Aircraft",
    )
    ax2.set_ylabel("Unique Aircraft", color="coral")
    ax2.tick_params(axis="y", labelcolor="coral")
    
    ax1.set_title(title)
    ax1.set_xticks(range(24))
    
    fig.tight_layout()
    
    if save_path:
        plt.savefig(save_path, dpi=150)
        logger.info(f"Saved plot to {save_path}")
    else:
        plt.show()
    
    plt.close()


def plot_altitude_distribution(
    df: pl.DataFrame,
    save_path: str | None = None,
    title: str = "Altitude Distribution",
) -> None:
    """
    Plot altitude distribution.
    
    Args:
        df: Flight data with altitude
        save_path: Optional path to save the plot
        title: Plot title
    """
    if df.is_empty() or "baro_altitude" not in df.columns:
        logger.warning("Missing 'baro_altitude' column, skipping plot")
        return
    
    # Filter out nulls
    alt_data = df.filter(pl.col("baro_altitude").is_not_null())["baro_altitude"].to_numpy()
    
    if len(alt_data) == 0:
        logger.warning("No altitude data available")
        return
    
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    
    # Histogram
    axes[0].hist(alt_data, bins=50, edgecolor="black", alpha=0.7)
    axes[0].set_xlabel("Altitude (m)")
    axes[0].set_ylabel("Frequency")
    axes[0].set_title("Altitude Histogram")
    
    # Box plot
    axes[1].boxplot(alt_data, vert=True)
    axes[1].set_ylabel("Altitude (m)")
    axes[1].set_title("Altitude Box Plot")
    
    fig.suptitle(title)
    plt.tight_layout()
    
    if save_path:
        plt.savefig(save_path, dpi=150)
        logger.info(f"Saved plot to {save_path}")
    else:
        plt.show()
    
    plt.close()


def plot_speed_distribution(
    df: pl.DataFrame,
    save_path: str | None = None,
    title: str = "Speed Distribution",
) -> None:
    """
    Plot speed distribution.
    
    Args:
        df: Flight data with velocity
        save_path: Optional path to save the plot
        title: Plot title
    """
    if df.is_empty() or "velocity" not in df.columns:
        logger.warning("Missing 'velocity' column, skipping plot")
        return
    
    # Filter out nulls and convert to km/h
    speed_data = df.filter(pl.col("velocity").is_not_null())["velocity"].to_numpy() * 3.6
    
    if len(speed_data) == 0:
        logger.warning("No speed data available")
        return
    
    fig, ax = plt.subplots(figsize=(12, 5))
    
    sns.histplot(speed_data, bins=50, kde=True, ax=ax)
    ax.set_xlabel("Speed (km/h)")
    ax.set_ylabel("Frequency")
    ax.set_title(title)
    
    # Add vertical lines for typical speed ranges
    ax.axvline(x=200, color="orange", linestyle="--", label="Small aircraft (~200 km/h)")
    ax.axvline(x=800, color="red", linestyle="--", label="Commercial jets (~800 km/h)")
    ax.legend()
    
    plt.tight_layout()
    
    if save_path:
        plt.savefig(save_path, dpi=150)
        logger.info(f"Saved plot to {save_path}")
    else:
        plt.show()
    
    plt.close()


def create_analysis_report(
    df: pl.DataFrame,
    output_dir: str = "reports",
    date_str: str = "analysis",
) -> None:
    """
    Create a full analysis report with all plots.
    
    Args:
        df: Flight data DataFrame
        output_dir: Directory to save plots
        date_str: Date string for file naming
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Creating analysis report in {output_path}")
    
    # Generate all plots
    plot_geographic_distribution(
        df,
        save_path=str(output_path / f"{date_str}_geographic.png"),
        title=f"Flight Distribution - {date_str}",
    )
    
    plot_hourly_traffic(
        df,
        save_path=str(output_path / f"{date_str}_hourly.png"),
        title=f"Hourly Traffic Pattern - {date_str}",
    )
    
    plot_altitude_distribution(
        df,
        save_path=str(output_path / f"{date_str}_altitude.png"),
        title=f"Altitude Distribution - {date_str}",
    )
    
    plot_speed_distribution(
        df,
        save_path=str(output_path / f"{date_str}_speed.png"),
        title=f"Speed Distribution - {date_str}",
    )
    
    logger.info(f"Analysis report complete: {output_path}")


__all__ = [
    "plot_geographic_distribution",
    "plot_hourly_traffic",
    "plot_altitude_distribution",
    "plot_speed_distribution",
    "create_analysis_report",
]
