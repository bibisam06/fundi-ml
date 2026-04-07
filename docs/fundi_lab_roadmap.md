# FUNDI Lab Roadmap

FUNDI는 랩 지원용 포트폴리오로도 충분히 강점이 있는 주제다. 특히 금융과 AI 어느 쪽 랩에 지원하더라도 아래처럼 메시지를 다르게 가져갈 수 있다.

## 1. 랩 지원 포지셔닝

- 금융/퀀트 성향 랩: `market microstructure`, `perpetual futures pricing`, `signal validity`, `out-of-sample validation`을 강조
- AI/데이터마이닝 성향 랩: `time-series feature engineering`, `regime detection`, `weak predictive signals`, `reproducible ML pipeline`을 강조
- 공통 메시지: "단순 시세 예측"이 아니라 "시장 구조에서 나온 지표를 통계적으로 검증하는 연구형 프로젝트"라는 점이 강점

## 2. 지금 프로젝트의 강점

- 연구 질문이 명확하다: `FR 극단값이 4시간 후 가격 방향과 관련이 있는가?`
- 방법 비교가 있다: `z-score`, `rolling z-score`, `quantile`
- 실패해도 의미가 있다: 예측력이 없다는 결론 자체가 연구 기여가 될 수 있다
- 구현이 이미 시작되어 있다: 데이터 수집, 병합, 피처 생성까지 파이프라인 기반으로 이어지고 있다

## 3. 우선 보강하면 좋은 부분

- Open Interest 수집 추가: 계획서의 핵심 변수인데 현재 코드에는 아직 없음
- 검증 분리: in-sample 결과와 out-of-sample 결과를 나눠야 연구 설득력이 올라감
- 통계 검정 추가: 평균 비교와 상관 분석만이 아니라 permutation test 또는 bootstrap도 고려 가능
- 시각화 정리: extreme event 전후 누적 수익률, regime별 hit ratio 그래프를 만들면 발표력이 좋아짐
- 재현성 강화: 실행 순서를 `collector -> processor -> evaluator` 형태로 문서화하고 테스트 최소 1개 추가

## 4. 다음 개발 우선순위

1. `signal_evaluator.py`로 extreme 정의별 방향성 비교 자동화
2. Open Interest 수집기 추가
3. Train/Test 기간 분리 및 out-of-sample 평가
4. Rule-based regime을 baseline으로 두고, 이후 HMM regime detection 확장
5. XGBoost 같은 ML 모델은 마지막 단계에서 baseline 대비 추가

## 5. 자기소개/면담 때 쓸 수 있는 한 줄

- "암호화폐 영구선물 시장의 펀딩비를 단순 상관관계가 아니라 regime과 extreme-event 관점에서 검증하는 프로젝트를 진행하고 있습니다."
- "시장 구조에서 정의된 신호가 실제로 단기 방향성을 갖는지, 그리고 그 효과가 시장 상태에 따라 달라지는지를 재현 가능한 파이프라인으로 분석하고 있습니다."
