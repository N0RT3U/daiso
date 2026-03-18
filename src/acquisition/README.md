# 다이소 뷰티 크롤링 시스템

다이소몰 뷰티 제품의 **제품 정보, 리뷰, 전성분**을 자동 수집하는 통합 크롤링 시스템

## 기술 스택

| 분류 | 기술 |
|------|------|
| 크롤링 | Selenium, undetected-chromedriver |
| OCR | Naver Clova OCR API, EasyOCR |
| 이미지 처리 | OpenCV, Pillow |
| 데이터 처리 | Pandas |
| 데이터 적재 | Google BigQuery (선택) |

## 핵심 기능

### 1. 다중 소스 성분 추출 (교차 검증)

제품 이미지에서 전성분을 추출하기 위해 **ALT 텍스트 + OCR 이중 소스**를 활용합니다.

```
[제품 상세 페이지]
       │
       ├── 소스 1: 이미지 ALT 속성 ──→ 성분 키워드 감지 → 파싱
       │                                    │
       └── 소스 2: OCR ──────────────→ 이미지 유형 판별
                                            │
                                    ┌───────┴───────┐
                                    │               │
                             멀티 이미지       싱글(긴) 이미지
                           (마지막 2장 OCR)   (Bottom-Up 3920px)
                                    │               │
                                    └───────┬───────┘
                                            │
                                    교차 검증 + 신뢰도 필터링 (≥0.5)
                                            │
                                    최종 성분 리스트 출력
```

**주요 기술적 해결:**
- **Clova OCR 1960px 제한 우회:** 긴 이미지를 Bottom-Up으로 3920px 분할 처리
- **OCR 오인식 자동 교정:** 280+ 규칙 (`소톱→소듐`, `글라이골→글라이콜` 등)
- **성분표 영역 자동 감지:** OpenCV 컨투어 기반 + 키워드 탐색

### 2. 증분 크롤링

기존 수집 이력을 JSON으로 관리하여, 신규 리뷰만 효율적으로 수집합니다.

```
첫 실행:  전체 크롤링 → crawl_history.json 생성
2회차~:   기존 제품 → 마지막 리뷰 날짜 이후만 수집 (cutoff)
          신규 제품 → 전체 크롤링 (제품 정보 + 리뷰 + 성분)
```

### 3. BigQuery 기반 리뷰 동기화

BQ `products_core` 테이블에서 전체 제품 코드를 조회하고, 각 제품의 신규 리뷰만 증분 수집합니다.

## 디렉토리 구조

```
01_crawling/
├── daiso_beauty_crawler.py    # 메인 크롤러 엔진 (1,365줄)
│                                ├─ extract_ingredients_multi_source()  성분 다중 소스 추출
│                                ├─ crawl_product_detail()              제품 상세 크롤링
│                                ├─ run_all()                           자동 전체/증분 크롤링
│                                └─ run_all_bq()                        BQ 기반 리뷰 동기화
│
├── config.py                  # 크롤링 설정 (카테고리 6개 중분류, 30+ 소분류)
├── crawl_history.py           # 증분 크롤링 이력 관리 (JSON)
├── utils.py                   # 유틸리티 (로거, 스크롤, 가격/평점 파싱)
├── requirements.txt           # 의존성
│
└── modules/                   # 기능별 모듈
    ├── driver_setup.py        # Selenium 드라이버 (봇 탐지 회피)
    ├── clova_ocr.py           # Naver Clova OCR V2 API 연동
    ├── ocr_utils_split.py     # 이미지 분할 OCR (EasyOCR)
    ├── image_preprocessor.py  # 이미지 전처리 (대비, 선명도, CLAHE)
    ├── ingredient_parser.py   # 성분명 파싱 + OCR 오인식 교정 (280+ 규칙)
    ├── ingredient_detector.py # 성분표 영역 자동 감지 (OpenCV)
    └── ingredient_postprocessor.py  # OCR 결과 노이즈 제거
```

## 데이터 흐름

```
[다이소몰 웹사이트]
       │
       ▼
[Selenium WebDriver] ─── undetected-chromedriver (봇 탐지 회피)
       │
       ├─ 1. 카테고리별 제품 링크 수집 (스크롤 로딩)
       │
       ├─ 2. 제품별 상세 크롤링
       │     ├─ 기본 정보 (브랜드, 제품명, 가격, 좋아요/공유)
       │     ├─ 리뷰 (최신순 정렬, 증분 cutoff 지원)
       │     └─ 성분 (ALT + OCR 교차 검증)
       │
       ├─ 3. CSV 저장 (50개마다 중간 저장)
       │     ├─ products_all_YYYYMMDD.csv
       │     ├─ reviews_all_YYYYMMDD.csv
       │     └─ ingredients_all_YYYYMMDD.csv
       │
       └─ 4. BigQuery 적재 (선택)
```

## 수집 규모

| 항목 | 수치 |
|------|------|
| 대상 카테고리 | 6개 중분류, 30+ 소분류 |
| 수집 제품 수 | 937개 |
| 수집 리뷰 수 | 323,114건 |
| OCR 교정 규칙 | 280+ |
| 총 코드 라인 | ~2,500줄 (메인 크롤러 + 모듈) |

## 핵심 코드 설명

### 성분 다중 소스 추출 (`daiso_beauty_crawler.py:67`)

```python
def extract_ingredients_multi_source(driver, product_code, product_name):
    """
    1. ALT 텍스트에서 성분 섹션 감지 (헤더 키워드 or 대표 성분명 3개+)
    2. OCR 실행 (멀티 이미지 → 마지막 2장, 싱글 이미지 → Bottom-Up 3920px)
    3. 교차 검증: 양쪽 모두 발견된 성분은 신뢰도 +0.1
    4. 최종 필터링: 신뢰도 ≥ 0.5만 포함
    """
```

### 증분 크롤링 (`daiso_beauty_crawler.py:1037`)

```python
def run_all(crawl_reviews=True, crawl_ingredients=True, history=None):
    """
    - history=None → 풀 크롤링 (전체 제품)
    - history=CrawlHistory → 증분 크롤링
        - 기존 제품: 리뷰만 업데이트 (cutoff 이후)
        - 신규 제품: 전체 크롤링
    - 50개마다 중간 저장 (에러 시 데이터 유실 방지)
    """
```

### OCR 오인식 교정 (`modules/ingredient_parser.py`)

```python
# 280+ 교정 규칙 (예시)
OCR_CORRECTION_MAP = {
    '소톱': '소듐',           # Sodium
    '글라이골': '글라이콜',   # Glycol
    '폴리솔베': '폴리소르베', # Polysorbate
    '스티아릭': '스테아릭',   # Stearic
    '다이을': '다이올',       # Diol
    ...
}
```
