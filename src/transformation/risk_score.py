import numpy as np
import pandas as pd


def calculate_risk_score(
    df: pd.DataFrame,
) -> pd.DataFrame:

    df = df.copy()

    # VOLATILITY RISK
    
    df["volatility_score"] = (
        df["volatility_24"]
        .rank(pct=True)
        .mul(100)
    )

    # DRAWDOWN RISK
    
    drawdown_risk = (
        df["drawdown_168"]
        .abs()
    )

    df["drawdown_score"] = (
        drawdown_risk
        .rank(pct=True)
        .mul(100)
    )

    # MOMENTUM RISK
    
    momentum_risk = (
        -df["momentum_24"]
    ).clip(lower=0)

    df["momentum_score"] = (
        momentum_risk
        .rank(pct=True)
        .mul(100)
    )

    # LIQUIDITY / TURNOVER
    
    df["turnover_ratio"] = (
        df["total_volume"]
        / df["market_cap"]
    )

    # Lower turnover = higher risk
    df["liquidity_score"] = (
        100
        - df["turnover_ratio"]
        .rank(pct=True)
        .mul(100)
    )

    # FINAL RISK SCORE
    

    df["risk_score"] = (
        df["volatility_score"] * 0.40
        + df["drawdown_score"] * 0.30
        + df["momentum_score"] * 0.20
        + df["liquidity_score"] * 0.10
    )

    df["risk_score"] = (
        df["risk_score"]
        .clip(0, 100)
        .round(2)
    )

    # RISK CATEGORY
    

    df["risk_level"] = pd.cut(
        df["risk_score"],
        bins=[
            -np.inf,
            25,
            50,
            75,
            np.inf,
        ],
        labels=[
            "LOW",
            "MODERATE",
            "HIGH",
            "VERY HIGH",
        ],
    )

    return df