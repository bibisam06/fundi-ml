# 💶 FUNDI - Funding Rate & Price Analysis

> Funding Rate를 활용해 BTC 선물 시장의 가격 방향성을 분석하는 데이터 기반 연구 프로젝트

---

## 📌 Overview

FUNDI는 암호화폐 선물 시장에서 **Funding Rate(FR)**와 가격 간의 관계를 분석하여  
**시장 심리와 가격 방향성을 설명할 수 있는지 검증**하는 프로젝트입니다.

기존 연구들이 단순 상관관계에 집중했다면,  
본 프로젝트는 다음을 중점적으로 다룹니다:

- 극단적인 Funding Rate 상황에서의 가격 반응
- 다양한 극단값 정의 방식 비교
- 시장 상태(regime)에 따른 신호 성능 변화

---

## 🧠 Research Question

> Can extreme funding rate conditions predict short-term BTC price direction?

- Time Horizon: **4 hours**
- Approach: Event-based + regime-aware analysis

---

## ⚙️ Tech Stack

- Python
- Pandas / NumPy
- Binance Futures API
- PostgreSQL (optional)
- Matplotlib / Plotly (planned)

---

## 📂 Project Structure

```
fundi-ml/
├── data/
│   ├── raw/           # 수집 데이터 (Git 제외)
│   └── processed/     # 전처리 데이터 (Git 제외)
├── reports/           # 분석 결과
├── src/
│   ├── data_collector.py
│   ├── data_processor.py
│   └── signal_evaluator.py
└── README.md
```

---

## 🔄 Pipeline

### 1. Data Collection
```bash
python src/data_collector.py
```

### 2. Data Processing
```bash
python src/data_processor.py
```

### 3. Signal Evaluation
```bash
python src/signal_evaluator.py
```

---

## 📊 Example Output

```
method           | events | accuracy | avg_4h_return | best_regime
---------------------------------------------------------------
zscore           |  XXX   |  XX%     |  XX%          | bull_high_vol
rolling_zscore   |  XXX   |  XX%     |  XX%          | bear_low_vol
quantile         |  XXX   |  XX%     |  XX%          | bull_low_vol
```

---

## 🧩 Key Features

- Funding Rate 기반 시장 상태 분석
- 극단값 정의 방식 비교
- Regime-aware signal evaluation
- 재현 가능한 데이터 파이프라인

---

## ⚠️ Notes

- data/ 디렉토리는 Git에 포함되지 않습니다.
- 아래 스크립트로 데이터 재생성 가능:

```bash
python src/data_collector.py
python src/data_processor.py
```

---

## 🚀 Future Work

- HMM 기반 Regime Detection
- 머신러닝 모델 (XGBoost)
- Cross-exchange 분석
- 실시간 시그널 시스템

---

## 👤 Author
bibisam06 : hb.jade00@gmail.com
Backend Developer interested in:
- Data Analysis
- Quantitative Research
- Financial Engineering

---

## 📌 Summary

This project explores whether funding rate extremes can act as a leading indicator  
for short-term price movements in cryptocurrency futures markets.
