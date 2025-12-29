"""
Hourly traffic forecasting model training.

Trains simple models to forecast flight traffic:
- Linear Regression
- Ridge Regression

Produces a report with MAE, MAPE for train and test sets.
"""

from datetime import date, timedelta
from pathlib import Path

import polars as pl
import numpy as np
from sklearn.linear_model import LinearRegression, Ridge, RidgeCV
from sklearn.preprocessing import StandardScaler, PolynomialFeatures
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split, TimeSeriesSplit, cross_val_score
from sklearn.metrics import mean_absolute_error, mean_absolute_percentage_error
from loguru import logger

from src.features.data import create_loader


# --- Feature Engineering ---

def create_hourly_aggregates(df: pl.DataFrame) -> pl.DataFrame:
    """Aggregate flight data to hourly traffic counts."""
    df = df.with_columns([
        pl.from_epoch(pl.col("capture_time")).alias("timestamp"),
    ])
    
    df = df.with_columns([
        pl.col("timestamp").dt.truncate("1h").alias("hour_start"),
    ])
    
    hourly = df.group_by("hour_start").agg([
        pl.col("icao24").n_unique().alias("flight_count"),
    ]).sort("hour_start")
    
    return hourly


def create_features(df: pl.DataFrame) -> pl.DataFrame:
    """Create all features for forecasting."""
    df = df.sort("hour_start")
    
    df = df.with_columns([
        # Time features
        pl.col("hour_start").dt.hour().alias("hour_of_day"),
        pl.col("hour_start").dt.weekday().alias("day_of_week"),
        (pl.col("hour_start").dt.weekday() >= 5).cast(pl.Int32).alias("is_weekend"),
        
        # Lag features
        pl.col("flight_count").shift(1).alias("lag_1h"),
        pl.col("flight_count").shift(24).alias("lag_24h"),
        
        # Rolling mean (6 hour window for quarter-day patterns, shifted by 1 to avoid leakage)
        pl.col("flight_count").shift(1).rolling_mean(window_size=6).alias("rolling_mean_6h"),
    ])
    
    # Drop nulls from lag and rolling features
    df = df.drop_nulls(subset=["lag_1h", "lag_24h", "rolling_mean_6h"])
    
    return df


# --- Data Loading ---

def load_date_range(start_date: date, end_date: date) -> pl.DataFrame:
    """Load flight data for a date range."""
    loader = create_loader()
    
    all_dfs = []
    current = start_date
    
    while current <= end_date:
        logger.info(f"Loading {current}...")
        try:
            df = loader.load_day(current)
            if not df.is_empty():
                all_dfs.append(df)
        except Exception as e:
            logger.warning(f"No data for {current}: {e}")
        current += timedelta(days=1)
    
    if not all_dfs:
        raise ValueError("No data loaded")
    
    combined = pl.concat(all_dfs, how="diagonal_relaxed")
    logger.info(f"Loaded {len(combined):,} total records")
    return combined


# --- Model Training ---

def train_and_evaluate():
    """Main training pipeline."""
    print("=" * 60)
    print("FLIGHT TRAFFIC FORECASTING - TRAINING")
    print("=" * 60)
    
    # 1. Load data (Dec 18-28)
    logger.info("Step 1: Loading data...")
    start_date = date(2025, 12, 18)
    end_date = date(2025, 12, 28)
    
    raw_df = load_date_range(start_date, end_date)
    
    # 2. Create hourly aggregates
    logger.info("Step 2: Creating hourly aggregates...")
    hourly = create_hourly_aggregates(raw_df)
    print(f"Hourly records: {len(hourly)}")
    
    # 3. Create features
    logger.info("Step 3: Creating features...")
    featured = create_features(hourly)
    print(f"Samples after feature engineering: {len(featured)}")
    
    # 4. Prepare X and y
    feature_cols = ["hour_of_day", "day_of_week", "is_weekend", "lag_1h", "lag_24h", "rolling_mean_6h"]
    target_col = "flight_count"
    
    X = featured.select(feature_cols).to_numpy()
    y = featured.select(target_col).to_numpy().flatten()
    
    # 5. Train/test split (80/20, preserve time order)
    split_idx = int(len(X) * 0.8)
    X_train, X_test = X[:split_idx], X[split_idx:]
    y_train, y_test = y[:split_idx], y[split_idx:]
    
    print(f"\nTrain size: {len(X_train)}, Test size: {len(X_test)}")
    
    # 6. Scale features for better performance
    logger.info("Step 4: Scaling features...")
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    
    # 7. Train models with hyperparameter tuning
    logger.info("Step 5: Training models...")
    
    # RidgeCV automatically finds best alpha
    alphas = [0.001, 0.01, 0.1, 1.0, 10.0, 100.0]
    ridge_cv = RidgeCV(alphas=alphas, cv=5)
    ridge_cv.fit(X_train_scaled, y_train)
    best_alpha = ridge_cv.alpha_
    print(f"\nRidgeCV best alpha: {best_alpha}")
    
    # Polynomial features (degree 2)
    poly = PolynomialFeatures(degree=2, include_bias=False)
    X_train_poly = poly.fit_transform(X_train_scaled)
    X_test_poly = poly.transform(X_test_scaled)
    print(f"Polynomial features: {X_train_poly.shape[1]} (from {X_train.shape[1]})")
    
    models = {
        "Linear (baseline)": (LinearRegression(), X_train, X_test),
        "Linear (scaled)": (LinearRegression(), X_train_scaled, X_test_scaled),
        "RidgeCV (α={:.3f})".format(best_alpha): (Ridge(alpha=best_alpha), X_train_scaled, X_test_scaled),
        "Linear + Poly(2)": (LinearRegression(), X_train_poly, X_test_poly),
        "Ridge + Poly(2)": (Ridge(alpha=best_alpha), X_train_poly, X_test_poly),
    }
    
    results = []
    
    for name, (model, X_tr, X_te) in models.items():
        logger.info(f"Training {name}...")
        model.fit(X_tr, y_train)
        
        # Predictions
        y_train_pred = model.predict(X_tr)
        y_test_pred = model.predict(X_te)
        
        # Metrics
        train_mae = mean_absolute_error(y_train, y_train_pred)
        test_mae = mean_absolute_error(y_test, y_test_pred)
        train_mape = mean_absolute_percentage_error(y_train, y_train_pred) * 100
        test_mape = mean_absolute_percentage_error(y_test, y_test_pred) * 100
        
        results.append({
            "Model": name,
            "Train MAE": train_mae,
            "Test MAE": test_mae,
            "Train MAPE (%)": train_mape,
            "Test MAPE (%)": test_mape,
        })
    
    # 7. Print report
    print("\n" + "=" * 60)
    print("MODEL COMPARISON REPORT")
    print("=" * 60)
    print(f"Date Range: {start_date} to {end_date}")
    print(f"Features: {feature_cols}")
    print(f"Target: {target_col}")
    print()
    
    results_df = pl.DataFrame(results)
    print(results_df)
    
    # Best model
    best = min(results, key=lambda x: x["Test MAE"])
    print(f"\nBest Model: {best['Model']}")
    print(f"  Test MAE: {best['Test MAE']:.2f} flights")
    print(f"  Test MAPE: {best['Test MAPE (%)']:.2f}%")
    print("=" * 60)
    
    # Save report
    report_path = Path("reports/forecasting_report.txt")
    report_path.parent.mkdir(exist_ok=True)
    
    with open(report_path, "w") as f:
        f.write("FLIGHT TRAFFIC FORECASTING REPORT\n")
        f.write("=" * 50 + "\n\n")
        f.write(f"Date Range: {start_date} to {end_date}\n")
        f.write(f"Train Size: {len(X_train)}, Test Size: {len(X_test)}\n")
        f.write(f"Features: {feature_cols}\n\n")
        f.write("Results:\n")
        f.write(str(results_df))
        f.write(f"\n\nBest Model: {best['Model']}\n")
        f.write(f"Test MAE: {best['Test MAE']:.2f}\n")
        f.write(f"Test MAPE: {best['Test MAPE (%)']:.2f}%\n")
    
    logger.info(f"Report saved to {report_path}")
    
    return results_df


if __name__ == "__main__":
    train_and_evaluate()
