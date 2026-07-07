# Options Scanner - GitHub Actions 자동화

러셀2000 종목 대상 옵션 거래량 급증(Unusual Options Activity) 스캐너를
매일 자동 실행하고 텔레그램으로 알림을 보내는 리포지토리입니다.

## 폴더 구조

```
.
├── .github/workflows/daily_scan.yml   # 자동 실행 설정 (건드릴 필요 없음)
└── scanner/
    ├── unusual_options_scanner.py     # 메인 스크립트
    ├── options_volume_history.csv     # 자동 생성/누적 (매일 커밋됨)
    └── unusual_options_today.csv      # 자동 생성 (매일 갱신됨)
```

## 처음 설정하는 방법 (한 번만 하면 됨)

### 1. GitHub 리포지토리 만들기

1. github.com에서 New repository 생성 (Public이든 Private이든 상관없음)
2. 이 폴더(`.github/`, `scanner/`) 통째로 그 리포지토리에 업로드
   - GitHub 웹에서 "Add file → Upload files"로 드래그해도 되고
   - `git` 명령어를 아는 경우 아래처럼:
     ```bash
     git init
     git add .
     git commit -m "initial commit"
     git remote add origin https://github.com/내계정/내리포.git
     git push -u origin main
     ```

### 2. 텔레그램 토큰/챗ID를 GitHub Secrets에 등록

1. 리포지토리 페이지 → **Settings** → 왼쪽 메뉴 **Secrets and variables** → **Actions**
2. **New repository secret** 클릭
   - Name: `TELEGRAM_BOT_TOKEN`, Value: 봇 토큰 붙여넣기 → Add secret
3. 같은 방식으로 하나 더 추가
   - Name: `TELEGRAM_CHAT_ID`, Value: 챗 ID 붙여넣기 → Add secret

이렇게 하면 토큰이 코드에 노출되지 않고 안전하게 저장됩니다.

### 3. 동작 확인 (수동 실행으로 테스트)

1. 리포지토리 → **Actions** 탭
2. 왼쪽에서 **Options Scanner Daily Run** 클릭
3. 오른쪽 **Run workflow** 버튼 클릭 → 실행
4. 몇 분 후 텔레그램으로 메시지가 오면 성공
5. 실행 로그는 Actions 탭에서 클릭해서 확인 가능 (에러 나면 여기서 원인 확인)

### 4. 이후에는?

- 설정된 cron 스케줄(평일 UTC 21:00 = 미국 동부 약 오후 5시)에 맞춰 자동 실행됩니다.
- 실행 결과(`options_volume_history.csv`, `unusual_options_today.csv`)는 자동으로
  리포지토리에 커밋되어 쌓입니다. 즉 히스토리가 GitHub 안에 안전하게 보관돼요.
- 서머타임/표준시 전환 시기(3월, 11월)에는 `.github/workflows/daily_scan.yml`의
  cron 시간을 한 번씩 조정해주면 더 정확합니다. (안 바꿔도 1시간 정도 차이만 남)

## 설정값 바꾸고 싶을 때

`scanner/unusual_options_scanner.py` 상단 CONFIG 섹션에서:
- `TOP_N`, `TELEGRAM_TOP_N`: 몇 개 종목까지 볼지
- `MIN_VOL_OI_RATIO`, `MIN_TOTAL_VOLUME`: 필터 기준
- `MAX_WORKERS`, `REQUEST_DELAY`: 속도/안정성 조절

수정 후 그냥 git push 하면 다음 실행부터 반영됩니다.

## 주의사항

- 이 스캐너는 참고용 스크리닝 도구이며 투자 자문이 아닙니다.
- yfinance는 비공식 데이터 소스라 간헐적 오류가 있을 수 있습니다.
- GitHub Actions 무료 티어는 Public 리포지토리는 무제한, Private은 매달 일정 시간
  한도가 있습니다(이 스캐너 정도 사용량은 무료 한도 내에서 충분합니다).
