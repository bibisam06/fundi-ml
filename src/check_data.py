import pandas as pd

funding = pd.read_csv("data/raw/btcusdt_funding_rate.csv")
kline = pd.read_csv("data/raw/btcusdt_1h_klines.csv")

print(funding.head())
print(funding.tail())
print(funding.info())

print(kline.head())
print(kline.tail())
print(kline.info())


if __name__ == "__main__":
    main()