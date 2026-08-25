"""배터리팩 품질검사 모니터링 대시보드.

데이터는 전부 Kafka 에서 온다. DB 를 직접 읽던 뼈대 단계는 끝났다.

    sensor_generator ─▶ battery.pack.measurement ─┬▶ 이 파일   (차트·수신 현황)
                                                  └▶ api ─▶ 판정(모델)
                                                        │
                              battery.pack.verdict ◀────┘
                                                  └▶ 이 파일   (타일 색·판정 카드·알림)

역할 분담(2026-08-24 결정):
  - 측정 토픽  -> 전압·온도 차트, 충전량, 수신 현황.  화면은 그리기만 한다.
  - 판정 토픽  -> 모듈 타일 색, 판정 카드, 알림.  판정은 api 가 detector 로
    한 번만 하고, 화면은 받은 state 와 지목을 그대로 칠한다.
    화면이 직접 판단하면 모델 도입 후 "타일은 정상인데 알림은 이상" 처럼
    갈라지기 때문이다.

모델은 이상 점수를 내지 않는다(2026-08-25 결정). 판정 메시지에 score /
threshold / module_scores 는 없다. 화면이 쓰는 것은 state 와 module·cell
지목뿐이라, 타일 16개 중 지목된 하나에만 상태 색이 들어간다.

화면은 st.fragment(run_every) 로 몇 초마다 스스로 다시 그려진다. 새 메시지가
Kafka 로 흘러들면 사람이 새로고침하지 않아도 화면이 따라간다.
"""

import os
import uuid
from datetime import datetime, timedelta, timezone

import altair as alt
import pandas as pd
import streamlit as st

# 공용 패키지 (PYTHONPATH=/workspace/src).
# consumer 만 쓴다 - 판정은 전부 api 가 하므로 화면은 detector 를 부르지 않는다.
from battery_pack_defect_detection import consumer as kc

# --------------------------------------------------------------------------
# 상수
# --------------------------------------------------------------------------

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")

# 화면이 스스로 다시 그려지는 주기. 발행이 3초에 1건이므로 그에 맞췄다.
REFRESH_EVERY = "3s"

# 데이터의 측정 간격(초). 창 길이를 시간으로 환산할 때 쓴다.
RESAMPLE_SECONDS = 5

MODULE_COUNT = 16
CELLS_PER_MODULE = 11
TEMPS_PER_MODULE = 2

# 화면에 찍는 시각의 기준. 컨테이너에 TZ 가 없어 datetime.now() 는 UTC 를
# 돌려주므로(실측: 화면 05:06 / 실제 14:06), 기준을 코드에 박아 둔다.
# 한국은 1988년 이후 서머타임이 없어 고정 +09:00 이 곧 Asia/Seoul 이다.
# sensor_generator.KST 와 같은 값이지만 거기서 가져오지 않는다 - 그 모듈은
# database 를 import 하는데, 화면은 DB 에 붙지 않기 때문이다(docker-compose).
KST = timezone(timedelta(hours=9))

# 판정 메시지의 state(영어) -> 화면 표기(한글)
STATE_KO = {"anomaly": "이상", "warning": "주의", "normal": "정상"}

# 차트에 그릴 창 길이. 데이터가 5초 간격이므로 건수 x 5초 = 실제 시간이다.
# None 은 커서까지 전부 - 구간 하나가 1,000~2,500건이라 2~3시간 분량이다.
WINDOW_CHOICES = {"10분": 120, "30분": 360, "1시간": 720, "3시간": 2160, "전체": None}
WINDOW_DEFAULT = "전체"

# goorm Reference Design System (DESIGN.md) 의 토큰.
# 차가운 회색 캔버스 + 단일 파란 액션색 + 그림자 없는 헤어라인 체계다.
#
# warn 전경색 주의: DESIGN.md 는 warning 틴트(#ffd9c8)만 정의하고 전경색을
# 주지 않는다. 색을 지어내지 않고 명시된 Ink 를 그 틴트에 짝지었다.
PALETTE = {
    # 중립 / 표면
    "bg": "#f7f7f7",        # Surface Grey  - 페이지 배경
    "card": "#ffffff",      # Canvas White  - 카드 표면
    "line": "#e1e1e1",      # Hairline      - 유일한 분리 장치
    "strong": "#c6c6c6",    # Border Strong - 강조된 테두리
    # 잉크 계단
    "ink": "#262626",        # 본문 / 제목 (순검정 아님)
    "slate": "#4c4c4c",      # Secondary Slate
    "muted": "#5d5d5d",      # Muted Slate - 캡션
    "faint": "#a3a3a3",      # Faint Grey  - 최저 강조
    # 액션 / 선택 (파랑 하나뿐)
    "action": "#2a72e5",     # Vapor Blue  - 기본 액션
    "select": "#0957c8",     # Active Blue - 선택 / 활성
    "link": "#0043b3",       # Link Blue
    # 의미색 (상태 표시 전용, 장식으로 쓰지 않는다)
    "alert": "#da3944", "alert_soft": "#ffd8d7",   # Danger
    "ok_fg": "#058765", "ok_soft": "#bbecd7",      # Success
    "warn": "#262626", "warn_soft": "#ffd9c8",     # Warning (전경은 Ink)
    "info_soft": "#c6e6ff",
    # 차트
    "ok": "#a3a3a3",         # 배경 채널선 = Faint Grey
    "cool": "#2a72e5",       # T01 = Vapor Blue
    "cool2": "#4c4c4c",      # T02 = Secondary Slate (두 번째 채도색을 만들지 않는다)
}

# 상태마다 (전경, 배경 틴트). DESIGN.md 의 의미색 짝을 그대로 쓴다.
# 판정 카드·도넛 범례·알림 배지가 같은 짝을 써야 한 팩의 상태가 한 색으로 읽힌다.
TONES = {
    "이상": (PALETTE["alert"], PALETTE["alert_soft"]),
    "주의": (PALETTE["warn"], PALETTE["warn_soft"]),
    "정상": (PALETTE["ok_fg"], PALETTE["ok_soft"]),
}

st.set_page_config(page_title="배터리팩 품질검사 모니터링",
                   layout="wide", initial_sidebar_state="collapsed")


# --------------------------------------------------------------------------
# 데이터 소스 - Kafka 구독
#
# st.cache_resource: 프로세스당 한 번만 실행된다. 브라우저 탭이 여러 개
# 열려도, 화면이 몇 초마다 다시 그려져도 컨슈머 스레드는 하나다.
# --------------------------------------------------------------------------

@st.cache_resource(show_spinner="Kafka 에 연결하는 중...")
def kafka_feeds():
    """측정·판정 두 토픽의 구독을 시작하고 버퍼 둘을 돌려준다.

    그룹 이름이 api 와 달라야 한다는 점이 중요하다 - Kafka 는 그룹이 다르면
    각자 전 건을 받는다(fan-out). api 와 같은 그룹으로 묶으면 파티션을 나눠
    갖게 되어 화면이 일부 팩만 보게 된다.
    """
    # 그룹 이름에 프로세스마다 다른 꼬리를 붙인다.
    #
    # api 와 다른 그룹이어야 양쪽이 전 건을 받는다(fan-out). 그런데 대시보드
    # 프로세스가 둘 이상일 때(개발 중 테스트 + 컨테이너, 또는 대시보드 2대)
    # 그룹이 같으면 파티션을 나눠 갖게 되어 각자 일부 팩만 보게 된다.
    # 화면은 늘 전체를 봐야 하므로 프로세스마다 그룹을 따로 쓴다.
    tag = uuid.uuid4().hex[:8]
    measurements, _ = kc.start(KAFKA_BROKER, f"streamlit-dashboard-{tag}")
    verdicts, _ = kc.start_verdicts(KAFKA_BROKER, f"streamlit-verdict-{tag}")
    return measurements, verdicts


def window_frame(measurements: kc.MeasurementBuffer, serial: int, mode: str,
                 size: int | None) -> pd.DataFrame:
    """버퍼에서 한 구간의 최근 size 건을 DataFrame 으로 꺼낸다.

    커서 슬라이더는 사라졌다 - 스트림이 곧 재생 위치라서, 화면은 늘
    버퍼의 끝(가장 최근 수신분)을 본다. size 는 차트가 뒤로 얼마나
    보여줄지만 정한다.
    """
    return pd.DataFrame(measurements.rows(serial, mode, limit=size))


def seconds_ago(iso: str | None) -> str:
    """ISO 시각 -> 'n초 전' 표기. 수신이 살아 있는지 한눈에 보이게 한다."""
    if iso is None:
        return "수신 전"
    delta = datetime.now(timezone.utc) - datetime.fromisoformat(iso)
    sec = int(delta.total_seconds())
    return f"{sec}초 전" if sec < 120 else f"{sec // 60}분 전"


# --------------------------------------------------------------------------
# 스타일
# --------------------------------------------------------------------------

st.markdown(f"""
<style>
  @import url('https://cdn.jsdelivr.net/gh/orioncactus/pretendard@v1.3.9/dist/web/variable/pretendardvariable-dynamic-subset.css');

  /* 한 가족 두 역할: 800 이 알리고 400 이 설명한다 */
  html, body, .stApp, [class*="css"] {{
      font-family:'Pretendard Variable', Pretendard, 'Apple SD Gothic Neo',
                  'Noto Sans KR', sans-serif; }}
  .stApp {{ background:{PALETTE['bg']}; color:{PALETTE['ink']}; }}
  .block-container {{ padding:1.6rem 2rem 3rem; max-width:1500px; }}
  #MainMenu, footer, header {{ visibility:hidden; }}

  .hdr {{ display:flex; justify-content:space-between; align-items:baseline;
          margin-bottom:1.1rem; }}
  .hdr h1 {{ font-size:1.25rem; font-weight:800; color:{PALETTE['ink']}; margin:0; }}
  .hdr .meta {{ font-size:.88rem; color:{PALETTE['muted']}; }}

  /* 카드 12px, 컨트롤 8px. 그림자 없이 헤어라인으로만 분리한다 */
  .card {{ background:{PALETTE['card']}; border:1px solid {PALETTE['line']};
           border-radius:12px; padding:1rem 1.15rem; box-shadow:none; }}
  .cap {{ font-size:.88rem; color:{PALETTE['muted']}; }}

  /* 판정 카드: 의미색 틴트 + 그에 짝지은 전경색 (DESIGN.md States) */
  .verdict {{ border:1px solid {PALETTE['line']}; border-radius:12px;
              padding:1.1rem 1.3rem; box-shadow:none; }}
  .verdict .tag {{ font-size:.88rem; opacity:.8; }}
  .verdict h2 {{ font-size:2rem; font-weight:800; margin:.15rem 0 .3rem; }}
  .verdict .sub {{ font-size:.88rem; opacity:.85; margin-bottom:1rem; }}
  .verdict .nums {{ display:flex; gap:2.2rem; }}
  .verdict .nums .k {{ font-size:.82rem; opacity:.75; }}
  .verdict .nums .v {{ font-size:1.25rem; font-weight:700;
                       font-variant-numeric:tabular-nums; }}

  .wx .t {{ font-size:2rem; font-weight:800; color:{PALETTE['ink']}; }}
  .wx .d {{ font-size:.88rem; color:{PALETTE['muted']}; }}

  /* 차트 카드 3종. st-key-* 는 st.container(key=) 가 만들어 준다 */
  .st-key-card-donut, .st-key-card-volt, .st-key-card-temp {{
      background:{PALETTE['card']}; border:1px solid {PALETTE['line']};
      border-radius:12px; padding:.8rem .9rem; box-shadow:none; }}

  /* 팩 선택 버튼: 8px 컨트롤, 헤어라인 테두리 */
  div[data-testid="stButton"] > button {{
      width:100%; text-align:left; justify-content:flex-start;
      background:{PALETTE['card']}; color:{PALETTE['ink']};
      border:1px solid {PALETTE['line']};
      border-radius:8px; padding:.5rem .7rem; margin:0 0 .35rem;
      font-size:.88rem; font-weight:500; line-height:1.4;
      white-space:normal; box-shadow:none;
      transition:border-color 120ms, background 120ms; }}
  div[data-testid="stButton"] > button:hover {{
      border-color:{PALETTE['strong']};
      background:{PALETTE['card']}; color:{PALETTE['ink']}; }}
  div[data-testid="stButton"] > button:focus:not(:active) {{
      color:{PALETTE['ink']}; box-shadow:none;
      outline:2px solid {PALETTE['action']}; outline-offset:1px; }}

  /* 선택된 팩 = Active Blue 채움 */
  div[data-testid="stButton"] > button[kind="primary"] {{
      background:{PALETTE['select']}; color:{PALETTE['card']}; font-weight:600;
      border-color:{PALETTE['select']}; }}
  div[data-testid="stButton"] > button[kind="primary"]:hover {{
      background:{PALETTE['select']}; color:{PALETTE['card']};
      border-color:{PALETTE['select']}; }}

  .st-key-packbox {{ border:none; padding:0; }}

  /* 선택 상자도 8px 컨트롤 규격에 맞춘다 */
  div[data-baseweb="select"] > div {{
      background:{PALETTE['card']}; border:1px solid {PALETTE['line']};
      border-radius:8px; font-size:.88rem; box-shadow:none; }}
</style>
""", unsafe_allow_html=True)


# --------------------------------------------------------------------------
# 화면
# --------------------------------------------------------------------------

def render_header(serial: int, mode: str) -> None:
    label = "충전" if mode == "chg" else "방전"
    st.markdown(f"""
    <div class="hdr">
      <h1>배터리팩 품질검사 모니터링</h1>
      <div class="meta">PACK {serial} · {label} · Kafka 실시간 수신 중</div>
    </div>""", unsafe_allow_html=True)


def render_sidebar(packs: pd.DataFrame) -> tuple[int, str]:
    """왼쪽 열: 날짜 카드 + 검사 대상 팩 선택. 고른 (serial, mode) 를 돌려준다.

    라디오 대신 버튼을 쓴다. 라디오는 동그라미를 CSS 로 숨겨야 하는데
    Streamlit 이 내부 DOM 을 바꾸면 그 셀렉터가 깨진다. 버튼은 숨길 것이
    없고 kind="primary" 로 선택 상태를 그대로 표현할 수 있다.
    """
    # naive 한 datetime.now() 를 쓰면 컨테이너의 UTC 가 그대로 찍힌다.
    now = datetime.now(KST)
    weekday = "월화수목금토일"[now.weekday()]
    st.markdown(
        '<div class="card wx" style="margin-bottom:.7rem;">'
        f'<div class="d">{now.year}년 {now.month}월 {now.day}일 ({weekday})</div>'
        f'<div class="t">{now:%H:%M}</div>'
        '<div class="d">라인 가동 07:00 –</div></div>',
        unsafe_allow_html=True)

    st.markdown('<div class="cap" style="margin:.2rem 0 .3rem;">검사 대상 팩</div>',
                unsafe_allow_html=True)

    # 100구간을 한 줄로 늘어놓으면 고르기 어렵다. 충전/방전을 먼저 나눈다.
    mode = st.segmented_control(
        "구간", ["chg", "dchg"], default="chg",
        format_func=lambda m: "충전" if m == "chg" else "방전",
        label_visibility="collapsed") or "chg"
    subset = packs[packs["mode"] == mode]

    # 아직 이 구간(충전/방전)의 메시지가 안 왔을 수 있다. 재생 순서가
    # 충전 전량 -> 방전 전량이라, 초반에는 방전 목록이 비어 있는 게 정상이다.
    if subset.empty:
        st.markdown('<div class="cap" style="margin:.4rem 0;">'
                    '이 구간의 측정이 아직 도착하지 않았습니다</div>',
                    unsafe_allow_html=True)
        other = packs.iloc[0]
        st.session_state.pack = (int(other.serial_number), other.mode)
        return st.session_state.pack

    first = (int(subset.iloc[0].serial_number), mode)
    if st.session_state.get("pack", (None, None))[1] != mode:
        st.session_state.pack = first

    # 버튼 라벨은 한 줄만 지원한다(마크다운 줄바꿈이 무시된다). 대신
    # st.button 의 key 가 만들어 주는 st-key-* 클래스에 ::after 로
    # 두 번째 줄을 붙인다.
    rules = "".join(
        f'.st-key-pk-{r.mode}-{r.serial_number} button::after'
        f'{{content:"{r.steps:,} 스텝";}}'
        for r in subset.itertuples(index=False))
    st.markdown(
        "<style>"
        'div[data-testid="stButton"] > button::after {'
        "  display:block; width:100%; margin-top:.15rem;"
        "  font-size:.7rem; font-weight:400; opacity:.7; }"
        f"{rules}</style>", unsafe_allow_html=True)

    # 팩을 2열로 늘어놓는다. 한 줄에 하나씩이면 목록이 지나치게 길어진다.
    # container(height=...) 가 스크롤 영역을 만들어 준다.
    with st.container(height=360, key="packbox"):
        rows = list(subset.itertuples(index=False))
        for start in range(0, len(rows), 2):
            for col, row in zip(st.columns(2, gap="small"), rows[start:start + 2]):
                with col:
                    key = (int(row.serial_number), row.mode)
                    if st.button(f"**PACK {row.serial_number}**",
                                 key=f"pk-{row.mode}-{row.serial_number}",
                                 type="primary" if key == st.session_state.pack
                                      else "secondary"):
                        st.session_state.pack = key
                        st.rerun()

    return st.session_state.pack


def render_verdict(verdict: dict | None, window: pd.DataFrame) -> None:
    """판정 카드. api 가 보낸 판정을 그대로 보여준다 - 여기서 계산하지 않는다.

    verdict 가 None 이면 아직 api 의 판정이 도착하지 않은 것이다. 측정은
    먼저 오고 판정은 api 를 거쳐 오므로, 켜자마자 잠깐은 대기가 정상이다.
    """
    if verdict is None:
        st.markdown(f"""
        <div class="verdict" style="background:{PALETTE['card']};color:{PALETTE['muted']};">
          <div class="tag">● 판정</div>
          <h2>판정 대기 중</h2>
          <div class="sub">api 의 판정 메시지를 기다리고 있습니다 —
          api 컨테이너가 떠 있는지 확인하세요</div>
        </div>""", unsafe_allow_html=True)
        return

    state = STATE_KO[verdict["state"]]
    duration = len(window) * RESAMPLE_SECONDS   # 차트 창이 담는 실제 시간

    tone = TONES[state]
    headline = {"이상": "이상 감지", "주의": "주의 관찰", "정상": "정상 범위"}[state]

    # 지목은 정상일 때 비어 있다(모델이 짚을 곳이 없다). 그때는 '–' 로 둔다.
    module, cell = verdict["module"], verdict["cell"]
    module_ko = f"M{module:02d}" if module is not None else "–"
    cell_ko = f"CV{cell:02d}" if cell is not None else "–"

    model = verdict["model"]
    st.markdown(f"""
    <div class="verdict" style="background:{tone[1]};color:{tone[0]};">
      <div class="tag">● 판정 · {model['name']} v{model['version']}</div>
      <h2>{headline}</h2>
      <div class="sub">{verdict['detail']} · seq {verdict['seq']:,}</div>
      <div class="nums">
        <div><div class="k">문제 모듈</div><div class="v">{module_ko}</div></div>
        <div><div class="k">문제 셀</div><div class="v">{cell_ko}</div></div>
        <div><div class="k">차트 구간</div><div class="v">{duration:,}초</div></div>
      </div>
    </div>""", unsafe_allow_html=True)


def render_donut(verdict: dict | None, window: pd.DataFrame, mode: str) -> None:
    """오른쪽 카드: 충전량 도넛 + 판정 지목.

    도넛의 각도는 최신 측정의 usoc_avg(사용자 표시용 충전량)다 - 목업의
    '62% 충전량' 그대로다. 아래 줄은 판정이 짚은 곳이다 - 모델이 모듈별
    점수를 내지 않으므로 개수 분포 대신 지목 하나를 보여준다.

    **이 값이 재생 진행도 노릇도 한다**(2026-08-25 결정). 구간이 끝나는
    자리가 SoC 로 정해져 있기 때문이다 - 충전은 34% 언저리에서 시작해
    100% 만충으로, 방전은 100% 에서 6% 로 끝난다. 그래서 따로 진행 막대를
    두지 않는다. 대신 두 가지를 반드시 붙인다:

      - 가운데 '충전량' 라벨. 라벨 없는 원형 게이지 + % 는 관습적으로
        진행도로 읽혀서, 실제로 41% 를 진행도로 오해한 일이 있었다.
      - 방향 표기. 방전 구간은 게이지가 **줄어들면서** 진행하므로,
        방향이 없으면 이번엔 '진행도가 거꾸로 간다' 로 읽힌다.
    """
    latest = window.iloc[-1]
    usoc = float(latest["usoc_avg"])

    src = pd.DataFrame({"k": ["충전", "남음"], "v": [usoc, 100 - usoc]})
    donut = (alt.Chart(src)
             .mark_arc(innerRadius=42, outerRadius=60, stroke="#fff", strokeWidth=2)
             .encode(theta=alt.Theta("v:Q", stack=True),
                     color=alt.Color("k:N", scale=alt.Scale(
                         domain=["충전", "남음"],
                         range=[PALETTE["action"], PALETTE["line"]]), legend=None),
                     order=alt.Order("k:N"))
             .properties(height=150))
    # 숫자만 두면 진행도로 읽힌다. 무엇의 % 인지 가운데에 같이 적는다.
    text = (alt.Chart(pd.DataFrame({"t": [f"{usoc:.0f}%"]}))
            .mark_text(size=22, fontWeight="bold", color=PALETTE["ink"], dy=-10)
            .encode(text="t:N"))
    unit = (alt.Chart(pd.DataFrame({"t": ["충전량"]}))
            .mark_text(size=11, color=PALETTE["muted"], dy=10)
            .encode(text="t:N"))

    # 차트 배경을 카드와 같은 흰색으로. 기본값이면 회색 페이지 위에
    # 흰 사각형이 얹혀 보인다. theme=None 은 Streamlit 테마 덮어쓰기 방지.
    layered = ((donut + text + unit)
               .configure(background=PALETTE["card"])
               .configure_view(strokeWidth=0))
    st.altair_chart(layered, width="stretch", theme=None)

    # SoC 가 어느 쪽으로 흐르는지. 충전은 만충(100%)에서, 방전은 하한(6%)
    # 에서 구간이 끝나므로 이 한 줄이 '얼마나 남았는지' 를 대신한다.
    # 하한 6% 는 데이터에서 관측된 값이라 상수로 박지 않고 방향만 적는다.
    heading = "충전 중 ↑" if mode == "chg" else "방전 중 ↓"

    # 판정 상태와 지목. 판정 전이면 대기로 둔다.
    if verdict is None:
        state, tone, target = "판정 대기", (PALETTE["muted"], PALETTE["card"]), "–"
    else:
        state = STATE_KO[verdict["state"]]
        tone = TONES[state]
        target = (f"M{verdict['module']:02d} CV{verdict['cell']:02d}"
                  if verdict["module"] is not None else "지목 없음")

    st.markdown(
        f"""<div style="text-align:center;font-size:.82rem;
                        color:{PALETTE['muted']};margin-top:-.6rem;">
              <span style="background:{tone[1]};color:{tone[0]};
                           border:1px solid {PALETTE['line']};border-radius:9999px;
                           padding:.1rem .6rem;font-weight:600;">{state}</span>
              &nbsp;{target}
              <div style="margin-top:.35rem;color:{PALETTE['ink']};
                          font-variant-numeric:tabular-nums;">
                seq {int(latest['seq']):,} · {heading}</div>
            </div>""", unsafe_allow_html=True)


def render_module_grid(verdict: dict | None, default: int) -> int:
    """모듈 16개 타일. 누르면 아래 차트가 그 모듈로 바뀐다.

    판정은 팩 단위라, 상태 색이 들어가는 타일은 **판정이 짚은 모듈 하나뿐**이다.
    나머지 15개는 중립색으로 두고 차트 전환 버튼 역할만 한다. 모델이 모듈별
    점수를 내지 않으므로 타일에 숫자를 쓰지 않는다 - 짚힌 타일에만 상태
    글자(이상/주의)가 들어간다.

    타일 자체를 버튼으로 만들고, 상태 색과 선택 표시를 st-key-* 클래스로 따로
    준다. Streamlit 버튼의 type 은 primary/secondary 둘뿐이라 상태 + 선택까지는
    표현할 수 없기 때문이다.
    """
    st.session_state.setdefault("module", default)

    # 판정이 짚은 모듈(0부터). 정상이거나 아직 판정 전이면 짚은 곳이 없다.
    flagged, flagged_state = None, None
    if verdict is not None and verdict["module"] is not None:
        flagged = verdict["module"] - 1
        flagged_state = STATE_KO[verdict["state"]]

    # 선택자 우선순위 주의:
    #   div[data-testid="stButton"] > button   -> (0,1,2)
    #   .st-key-mod-06 button                  -> (0,1,1)   ← 일반 규칙에 진다
    # 아래처럼 컨테이너 클래스 + testid 를 함께 써야(0,2,2) 색이 실제로 먹는다.
    def sel(m: int) -> str:
        return f'.st-key-mod-{m:02d} div[data-testid="stButton"] > button'

    rules = []
    for m in range(MODULE_COUNT):
        target = sel(m)
        marked = m == flagged
        # 둘째 줄은 짚힌 타일에만 상태를 쓴다. 나머지는 빈칸(CSS 의 \00a0,
        # 곧 nbsp)을 넣어 높이를 맞춘다 - 비우면 그 타일만 한 줄 낮아진다.
        label = flagged_state if marked else "\\00a0"

        if m == st.session_state.module:
            # 선택: Active Blue 로 채우고 글씨는 흰색
            blue, white = PALETTE["select"], PALETTE["card"]
            rules.append(f'{target},{target}:hover,{target}:focus'
                         f'{{background:{blue};border-color:{blue};'
                         f'color:{white};font-weight:600;}}')
            rules.append(f'{target}::after{{content:"{label}";color:{white};}}')
        else:
            # 상태는 의미색 틴트로, 선택은 Active Blue 로 보인다. 두 신호가
            # 서로 다른 축을 쓰므로 겹쳐도 헷갈리지 않는다.
            fg, fill = (TONES[flagged_state] if marked
                        else (PALETTE["ink"], PALETTE["card"]))
            edge = PALETTE["strong"] if marked else PALETTE["line"]
            rules.append(f'{target}{{background:{fill};border-color:{edge};'
                         f'color:{fg};}}')
            rules.append(f'{target}:hover{{background:{fill};'
                         f'border-color:{PALETTE["select"]};color:{fg};}}')
            rules.append(f'{target}::after{{content:"{label}";color:{fg};}}')

    st.markdown(
        "<style>"
        # 모듈 타일: 가로로 길게. 정사각형이면 글자가 줄바꿈된다.
        '[class*="st-key-mod-"] div[data-testid="stButton"] > button{'
        "text-align:center;justify-content:center;white-space:nowrap;"
        "padding:.65rem 1.1rem;min-height:0;height:auto;"
        "font-size:.88rem;line-height:1.2;border-radius:8px;margin:0 0 .4rem;}"
        # 둘째 줄(상태 글자). 숫자였을 때보다 작게 - 한글 두 글자가 들어간다.
        '[class*="st-key-mod-"] div[data-testid="stButton"] > button::after{'
        "display:block;width:100%;text-align:center;white-space:nowrap;"
        "font-size:.82rem;font-weight:600;opacity:1;margin-top:.15rem;}"
        + "".join(rules) + "</style>", unsafe_allow_html=True)

    for half in (0, 8):
        for col, m in zip(st.columns(8, gap="small"), range(half, half + 8)):
            with col:
                if st.button(f"M{m + 1:02d}", key=f"mod-{m:02d}"):
                    st.session_state.module = m
                    st.rerun()

    return st.session_state.module


def render_charts(window: pd.DataFrame, module: int) -> None:
    """아래 두 차트: 셀 전압 11채널, 모듈 온도 2채널.

    11개 선이 3.6~4.1V 안에 뭉쳐 있어 선 하나를 겨냥해 올리기가 어렵다.
    그래서 선이 아니라 x축(시각)을 기준으로 최근접을 잡고, 그 시점의 모든
    채널 값을 한 툴팁에 모아 보여준다. 커서를 대충 올려도 읽힌다.
    """
    volt_rows, temp_rows = [], []
    for _, row in window.iterrows():
        at = row["measured_at"]
        base = module * CELLS_PER_MODULE
        cells = row["cell_voltages"][base:base + CELLS_PER_MODULE]
        mean = sum(cells) / len(cells)
        for c, v in enumerate(cells):
            volt_rows.append({"t": at, "ch": f"CV{c + 1:02d}", "v": v,
                              # 모듈 평균과의 차이(mV). 뭉친 선을 벌려 준다.
                              "d": (v - mean) * 1000})
        for k in range(TEMPS_PER_MODULE):
            temp_rows.append({"t": at, "ch": f"T{k + 1:02d}",
                              "v": row["module_temps"][module * TEMPS_PER_MODULE + k]})

    volts = pd.DataFrame(volt_rows)
    temps = pd.DataFrame(temp_rows)
    channels = [f"CV{c + 1:02d}" for c in range(CELLS_PER_MODULE)]

    # 폭이 가장 큰 채널을 이탈 후보로 보고 색을 준다. 나머지는 회색 배경선.
    odd = volts.groupby("ch")["v"].agg(lambda x: x.max() - x.min()).idxmax()

    def guided(source: pd.DataFrame, value: str, fmt: str, chs: list[str],
               unit: str, colored: bool):
        """세로 가이드라인 + 전 채널 툴팁이 붙은 다중 선 차트."""
        base = alt.Chart(source).encode(
            x=alt.X("t:T", title=None, axis=alt.Axis(format="%H:%M")))

        color = (alt.condition(alt.datum.ch == odd,
                               alt.value(PALETTE["alert"]), alt.value(PALETTE["ok"]))
                 if colored else
                 alt.Color("ch:N", title=None, scale=alt.Scale(
                     domain=["T01", "T02"],
                     range=[PALETTE["cool"], PALETTE["cool2"]])) )

        lines = base.mark_line(strokeWidth=1.4).encode(
            y=alt.Y(f"{value}:Q", title=None, scale=alt.Scale(zero=False)),
            color=color, detail="ch:N")

        # 선이 아니라 x축 기준 최근접. 커서를 정확히 겹칠 필요가 없다.
        nearest = alt.selection_point(nearest=True, on="pointerover",
                                      fields=["t"], empty=False)

        # 가이드라인 하나가 그 시각의 모든 채널 값을 툴팁으로 물고 있다.
        rule = (base.transform_pivot("ch", value=value, groupby=["t"])
                .mark_rule(color=PALETTE["ink"], strokeWidth=1)
                .encode(opacity=alt.condition(nearest, alt.value(0.28), alt.value(0)),
                        tooltip=[alt.Tooltip("t:T", title="시각", format="%H:%M:%S")]
                                + [alt.Tooltip(c, type="quantitative",
                                               title=f"{c} ({unit})", format=fmt)
                                   for c in chs])
                .add_params(nearest))

        dots = lines.mark_point(size=40, filled=True).encode(
            opacity=alt.condition(nearest, alt.value(1), alt.value(0)))

        return ((lines + dots + rule)
                .properties(height=210)
                .configure(background=PALETTE["card"])
                .configure_view(strokeWidth=0))

    left, right = st.columns(2)
    with left:
        with st.container(border=True, key="card-volt"):
            head, ctrl = st.columns([3, 2])
            with head:
                st.markdown(f'<div class="cap">M{module + 1:02d} 셀 전압 '
                            f'{CELLS_PER_MODULE}채널</div>', unsafe_allow_html=True)
            with ctrl:
                dev = st.toggle("편차 보기", value=False, key="devmode",
                                help="절대 전압 대신 모듈 평균과의 차이(mV)를 그린다. "
                                     "3.6~4.1V 에 뭉쳐 있던 선이 벌어져 이탈이 바로 보인다.")
            st.altair_chart(
                guided(volts, "d" if dev else "v",
                       ".2f" if dev else ".3f", channels,
                       "mV" if dev else "V", colored=True),
                width="stretch", theme=None)

    with right:
        with st.container(border=True, key="card-temp"):
            st.markdown(f'<div class="cap">M{module + 1:02d} 모듈 온도 '
                        f'{TEMPS_PER_MODULE}채널 &nbsp;·&nbsp; °C</div>',
                        unsafe_allow_html=True)
            st.markdown('<div style="height:38px"></div>', unsafe_allow_html=True)
            st.altair_chart(guided(temps, "v", ".1f", ["T01", "T02"], "°C",
                                   colored=False),
                            width="stretch", theme=None)


# --------------------------------------------------------------------------
# 파이프라인 현황 / 알림
# --------------------------------------------------------------------------

def render_pipeline_status(measurements: kc.MeasurementBuffer,
                           verdicts: kc.VerdictBuffer) -> None:
    """왼쪽 아래: 두 토픽의 수신 현황. 파이프라인이 살아 있는지 여기서 본다.

    '마지막 수신 n초 전' 이 계속 커지면 generator 나 api 가 멈춘 것이다.
    """
    ms, vs = measurements.stats(), verdicts.stats()
    by = vs["by_state"]
    st.markdown(f"""
    <div class="card" style="margin-top:.9rem;font-size:.8rem;line-height:1.9;">
      <div class="cap">파이프라인 수신 현황</div>
      <div>측정&nbsp; <b style="font-variant-numeric:tabular-nums;">{ms['received']:,}</b>건
           · {seconds_ago(ms['last_at'])}</div>
      <div>판정&nbsp; <b style="font-variant-numeric:tabular-nums;">{vs['received']:,}</b>건
           · {seconds_ago(vs['last_at'])}</div>
      <div style="color:{PALETTE['muted']};">
        이상 {by.get('anomaly', 0)} · 주의 {by.get('warning', 0)}
        · 정상 {by.get('normal', 0)}
        · 유실 {ms['gaps']} · 중복 {ms['duplicates']}</div>
    </div>""", unsafe_allow_html=True)


def render_alerts(verdicts: kc.VerdictBuffer) -> None:
    """본문 아래: 최근 이상/주의 알림. api 가 낸 판정 중 봐야 할 것만 남는다."""
    st.markdown('<div class="cap" style="margin:1.1rem 0 .4rem;">최근 알림</div>',
                unsafe_allow_html=True)
    alerts = verdicts.recent_alerts(8)
    if not alerts:
        st.markdown(f'<div class="card" style="font-size:.85rem;'
                    f'color:{PALETTE["muted"]};">알림 없음 — 지금까지의 판정이 '
                    f'모두 정상 범위입니다</div>', unsafe_allow_html=True)
        return

    rows = []
    for a in alerts:
        state = STATE_KO[a["state"]]
        fg, bg = TONES[state]
        rows.append(
            f'<div style="display:flex;gap:.8rem;align-items:baseline;'
            f'padding:.45rem .2rem;border-bottom:1px solid {PALETTE["line"]};">'
            f'<span style="background:{bg};color:{fg};border-radius:9999px;'
            f'padding:.1rem .6rem;font-size:.72rem;font-weight:600;">{state}</span>'
            f'<b>PACK {a["serial_number"]}</b>'
            f'<span>{a["detail"]}</span>'
            f'<span style="color:{PALETTE["muted"]};font-size:.78rem;margin-left:auto;">'
            f'측정 {a["measured_at"][11:19]} · seq {a["seq"]:,}</span></div>')
    st.markdown(f'<div class="card" style="font-size:.85rem;">{"".join(rows)}</div>',
                unsafe_allow_html=True)


# --------------------------------------------------------------------------
# 조립
# --------------------------------------------------------------------------

@st.fragment(run_every=REFRESH_EVERY)
def dashboard() -> None:
    """본문 전체. run_every 덕에 몇 초마다 스스로 다시 그려진다.

    Kafka 컨슈머는 백그라운드 스레드에서 쉬지 않고 버퍼를 채우고,
    이 함수는 다시 그려질 때마다 버퍼의 최신 내용을 읽는다. 그래서
    사람이 새로고침하지 않아도 화면이 스트림을 따라간다.
    """
    measurements, verdicts = kafka_feeds()
    sections = measurements.sections()

    side, body = st.columns([1, 3.4], gap="medium")

    # ---- 아직 아무 측정도 안 왔을 때: 시작 안내 ----
    if not sections:
        with body:
            st.markdown('<div class="hdr"><h1>배터리팩 품질검사 모니터링</h1>'
                        '<div class="meta">Kafka 수신 대기 중</div></div>',
                        unsafe_allow_html=True)
            st.info("아직 수신한 측정이 없습니다. sensor generator 를 실행하면 "
                    "몇 초 안에 화면이 채워집니다.")
            st.code("docker compose exec dev python sensor_generator.py --limit 100",
                    language="bash")
        with side:
            render_pipeline_status(measurements, verdicts)
        return

    packs = pd.DataFrame(sections)

    # ---- 왼쪽: 팩 선택 / 차트 창 / 수신 현황 ----
    with side:
        serial, mode = render_sidebar(packs)

        st.markdown('<div class="cap" style="margin:.7rem 0 -.5rem;">'
                    '차트 표시 구간</div>', unsafe_allow_html=True)
        span = st.selectbox(
            "차트 표시 구간", list(WINDOW_CHOICES),
            index=list(WINDOW_CHOICES).index(WINDOW_DEFAULT),
            label_visibility="collapsed",
            help="차트가 스트림의 끝에서 뒤로 얼마만큼을 보여줄지 정한다. "
                 "데이터가 5초 간격이라 '10분' 은 120건이다.")

        render_pipeline_status(measurements, verdicts)

    # ---- 데이터 꺼내기: 측정은 차트로, 판정은 타일·카드로 ----
    window = window_frame(measurements, serial, mode, WINDOW_CHOICES[span])
    if window.empty:      # 팩 전환 직후 한 프레임 정도는 비어 있을 수 있다
        return
    verdict = verdicts.latest_for(serial, mode)

    # 팩을 바꾸면 모듈 선택을 그 팩의 판정이 가리키는 곳으로 되돌린다.
    # 지목이 없으면(정상이거나 판정 전) M01 부터 본다.
    flagged = verdict["module"] if verdict else None
    default = (flagged - 1) if flagged is not None else 0
    if st.session_state.get("module_for") != (serial, mode):
        st.session_state.module = default
        st.session_state.module_for = (serial, mode)

    # ---- 오른쪽 본문 ----
    with body:
        render_header(serial, mode)

        top_left, top_right = st.columns([2, 1], gap="medium")
        with top_left:
            render_verdict(verdict, window)
        with top_right:
            with st.container(border=True, key="card-donut"):
                render_donut(verdict, window, mode)

        st.markdown('<div class="cap" style="margin:1rem 0 .4rem;">'
                    f'모듈별 상태 · {MODULE_COUNT}개 &nbsp;·&nbsp; '
                    '눌러서 아래 차트를 바꾼다 · 색은 api 판정</div>',
                    unsafe_allow_html=True)
        module = render_module_grid(verdict, default)

        st.markdown("<div style='height:.8rem'></div>", unsafe_allow_html=True)
        render_charts(window, module)

        render_alerts(verdicts)

    # 하단 캡션: 판정이 어느 모델에서 왔는지 남긴다
    if verdict:
        st.caption(
            "측정 battery.pack.measurement · 판정 battery.pack.verdict 구독. "
            f"판정은 api 의 모델 {verdict['model']['name']} "
            f"v{verdict['model']['version']} 이 낸 것이다 (점수 없이 판정만 온다)")
    else:
        st.caption("측정 battery.pack.measurement · 판정 battery.pack.verdict 구독. "
                   "판정 대기 중")


def main() -> None:
    dashboard()


if __name__ == "__main__":
    main()
