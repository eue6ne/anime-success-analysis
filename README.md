# Anime Success Analysis

MyAnimeList Top 10,000 Anime Dataset을 활용하여 애니메이션 작품의 인기 요인을 통계적으로 분석한 프로젝트입니다.

---

## 1. Project Objective

본 프로젝트는 애니메이션 작품의 인기(log_members)에 영향을 미치는 요인을 통계적 선형 회귀 분석을 중심으로 설명하고, 추가적으로 Ridge 및 RandomForest 모델을 통해 예측 관점의 확장 가능성을 검증하는 것을 목표로 합니다.

단순 상관관계 분석을 넘어,
- 제작 구조
- 장르 다양성
- 작품 유형
- 연도 효과
등 산업적 변수들이 인기 형성에 어떤 영향을 미치는지 검증하였습니다.

---

## 2. Dataset

- Source: Kaggle - MyAnimeList Top 10,000 Anime Dataset
  - https://www.kaggle.com/datasets/furkanark/myanimelist-top-10000-anime-dataset
- Platform: MyAnimeList (MAL)
- Observations: 9,999
- 주요 변수:
  - score
  - members (log 변환하여 사용)
  - genre_count
  - producer_count
  - studio_count
  - voice_actor_count
  - year
  - type (categorical)

---

## 3. Methodology

1. 데이터 전처리 및 feature engineering
2. 탐색적 데이터 분석 (EDA)
3. OLS 회귀 모델 구축
4. 다중공선성 검증 (VIF)
5. 변수 정제 (비유의 변수 제거)
6. 잔차 진단을 통한 모델 가정 검증
7. Ridge Regression을 통한 규제 선형 모델 비교
8. RandomForest를 통한 비선형 모델 확장 및 성능 비교
9. Feature Importance 분석

---

## 4. Key Findings

- 평점(score)은 인기의 가장 강력한 설명 변수로 나타났다.
- 제작 참여 규모(producer_count)는 인기와 유의한 양의 관계를 보였다.
- 복합 장르 작품은 평균적으로 더 높은 인기를 보였다.
- 최근 작품일수록 높은 인기를 보이는 경향이 확인되었다.
- 성우 참여 인원 수는 상대적으로 영향력이 제한적이었다.
- RandomForest 모델 적용 시 Test R²가 약 0.716까지 향상되었으며, 이는 인기 형성 구조에 비선형적 특성이 존재할 가능성을 시사한다.
- Ridge Regression은 OLS와 유사한 성능을 보였으며, 본 데이터에서는 강한 규제가 필요하지 않았다.

해석 중심 OLS 모델의 설명력(R²)은 약 0.616이었으며, 비선형 확장(RandomForest) 모델에서는 약 0.716까지 향상되었다.
이는 인기 형성 구조가 완전한 선형 관계에 국한되지 않음을 시사한다.
또한 RandomForest는 예측 성능은 우수하나, 계수 기반의 해석은 제한적이라는 한계가 있다.

---

## 5. Limitations

본 분석은 상관 관계 기반 설명 모델이며, 인과 관계를 직접적으로 증명하는 것은 아닙니다.

---

## 6. Repository Structure

```
anime-success-analysis/
│
├── data/                     # (raw / processed는 Git에 포함되지 않음)
│
├── notebooks/
│   ├── 01_eda.ipynb
│   ├── 02_modeling.ipynb
│   └── 03_model_extension.ipynb
│
├── sql/
│   ├── 01_schema_extended.sql
│   ├── 02_1_create_raw_entities.sql
│   ├── 02_2_load_entities_master.sql
│   ├── 05_build_features.sql
│   ├── 06_analysis_queries.sql
│   └── legacy/
│
├── src/
│   ├── etl_pipeline.py        # Raw → DB 적재
│   ├── make_dataset.py        # Feature dataset 생성
│   ├── train_models.py        # OLS / Ridge / RF 학습
│   └── legacy/
│
├── scripts/
│   └── inspect_raw_csvs.py
│
├── requirements.txt
├── .gitignore
└── README.md
```

※ data/raw/, data/processed/, reports/ 디렉토리는 대용량 파일 및 보안 이슈로 Git에 포함하지 않았습니다.

---

## 7. How to Run

### 1. Clone repository

```bash
git clone https://github.com/eue6ne/anime-success-analysis.git
cd anime-success-analysis
```

### 2. Create virtual environment

```bash
python -m venv venv
source venv/bin/activate  # macOS / Linux
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Set environment variables

Create .env file in project root:
```
DB_HOST=localhost
DB_USER=your_id
DB_PASSWORD=your_password
DB_NAME=anime_project
```

### 5. Create Database Schema

```bash
mysql -u your_id -p < sql/01_schema_extended.sql
mysql -u your_id -p < sql/02_1_create_raw_entities.sql
```

### 6. Run ETL pipeline

Load anime, genres, companies, characters, and voice actors.

```bash
python src/etl_pipeline.py
```

이 단계에서 다음 테이블이 모두 적재됩니다:
	•	anime_dim
	•	genre_dim / anime_genre_map
	•	entities
	•	company / anime_company
	•	anime_character
	•	anime_voice_actor

### 7. Verify Data Load (Optional Check)

```bash
mysql -u your_id -p -e "
USE anime_project;
SELECT COUNT(*) AS anime_character_cnt FROM anime_character;
SELECT COUNT(*) AS anime_voice_actor_cnt FROM anime_voice_actor;
"
```

정상 실행 시 다음과 유사한 결과가 출력됩니다:
	•	anime_character_cnt ≈ 79,654
	•	anime_voice_actor_cnt ≈ 155,938


### 8. Load Entity Master Tables

```bash
mysql -u your_id -p < sql/02_2_load_entities_master.sql
```

### 9. Build Feature View

```bash
mysql -u your_id -p < sql/05_build_features.sql
```

### 10. Generate Dataset

```bash
python src/make_dataset.py
```

### 11. Train Models

```bash
python src/train_models.py
```

---

## 8. Author

Personal Data Analysis Project  
2026