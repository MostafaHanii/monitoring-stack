# Cryptocurrency Dashboard User Guide

This guide explains every component of your **Cryptocurrency OHLC Live Dashboard** and how to use it for real-time trading insights.

## 1. Overview Section
*Quick snapshots of the market's current state.*

- **Latest Open/Close Price**: The most recent price data from the 1-minute candles.
- **Highest/Lowest Price (1h)**: The range of price movement over the last hour.
    - **💡 Insight**: If the current price is near the "Highest" value, momentum is strong. If near "Lowest", it's weak.
- **Volatility**: Measures how much the price is fluctuating.
    - **💡 Insight**: High volatility (>0.5%) means risky but potentially profitable price swings. Low volatility means the market is ranging/flat.
- **1h Price Change %**: The percentage difference between the price now and 1 hour ago.
    - **💡 Insight**: Green (>0%) = Bullish trend over the last hour. Red (<0%) = Bearish trend.

## 2. Price Action Section
*Detailed analysis of price movements and trends.*

### Candlestick Chart
- **Visual**:
    - **Green**: Price is higher than the previous candle's close.
    - **Red**: Price is lower than the previous candle's close.
    - **Hollow (Empty)**: Close > Open (Price went UP during this specific minute).
    - **Filled (Solid)**: Close < Open (Price went DOWN during this specific minute).
- **Action**: Look for the **"Trade on Binance"** link in the top-right corner of the panel (or in the panel menu). Clicking it takes you to the trading page.
- **💡 Insight**: Look for patterns. A long green bar engulfing a previous red bar is a strong **Buy Signal**. Long "wicks" (thin lines) indicate rejection of a price level.

### Bollinger Bands
- **Visual**: Three lines—Upper Band, Lower Band, and a central Moving Average.
- **💡 Insight**:
    - **Squeeze**: When bands get tight, a big price move (explosion) is coming.
    - **Overextended**: If price touches the **Upper Band**, it might be overbought (Sell). If it touches the **Lower Band**, it might be oversold (Buy).

### VWAP (Volume Weighted Average Price)
- **Visual**: A purple line overlaying the price chart.
- **💡 Insight**:
    - **Institutional Level**: VWAP is often used by institutions as a benchmark.
    - **Support/Resistance**: Price often bounces off the VWAP line.
    - **Trend**: Price > VWAP is Bullish. Price < VWAP is Bearish.

### Moving Averages (SMA)
- **Visual**: Two lines overlaying the price chart.
    - **SMA 50m**: Faster average (Short-term trend).
    - **SMA 200m**: Slower average (Long-term trend).
- **💡 Insight**:
    - **Golden Cross (Buy)**: 50m crosses **ABOVE** 200m.
    - **Death Cross (Sell)**: 50m crosses **BELOW** 200m.

## 3. Volume & Indicators Section
*Momentum and strength confirmation.*

### Base Volume & SMA
- **Visual**: Bars (Volume) with a Yellow Line (SMA).
- **💡 Insight**:
    - **Volume Spike**: If a bar is significantly higher than the yellow line, it's a high-volume event.
    - **Trend Confirmation**: High volume on price moves confirms the trend.

### Market Overview Table
- **Visual**: A dedicated section at the bottom with a table showing all monitored coins.
- **Columns**: Price, 1h Change %, 24h Volume.
- **💡 Insight**: Quickly scan for the "Mover of the Day" (highest change %) without switching charts.

### DSI Approximation (Price Momentum)
- **Visual**: Line showing the difference between current price and price 10 minutes ago.
- **💡 Insight**:
    - **Positive**: Price is higher than 10m ago (Upward Momentum).
    - **Negative**: Price is lower than 10m ago (Downward Momentum).

### Relative Strength Index (RSI)
- **Visual**: Line oscillates between 0 and 100.
    - **Red Zone (>70)**: Shaded background indicating Overbought.
    - **Green Zone (<30)**: Shaded background indicating Oversold.
    - **Line Color**: Changes from Green (<30) to Yellow (30-70) to Red (>70).
- **💡 Insight**:
    - **Overbought**: Price rose too fast. Expect a drop. **(Sell Signal)**
    - **Oversold**: Price fell too fast. Expect a bounce. **(Buy Signal)**

### MACD (Moving Average Convergence Divergence)
- **Visual**: MACD Line vs. Signal Line.
- **💡 Insight**:
    - **Bullish**: MACD crosses **ABOVE** Signal line.
    - **Bearish**: MACD crosses **BELOW** Signal line.

## Summary Checklist
| Indicator | Buy Signal 🟢 | Sell Signal 🔴 |
| :--- | :--- | :--- |
| **Trend** | Price > SMA 200 | Price < SMA 200 |
| **Momentum** | RSI < 30 | RSI > 70 |
| **Confirmation** | MACD > Signal | MACD < Signal |
| **Volume** | High on Green Candle | High on Red Candle |
