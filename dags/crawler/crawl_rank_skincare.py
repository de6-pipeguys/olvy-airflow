from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import datetime
from bs4 import BeautifulSoup
from airflow.utils.log.logging_mixin import LoggingMixin


def get_top100_skincare() -> tuple:
    log = LoggingMixin().log
    log.info("[get_top100_skincare] 시작")
    chrome_options = Options()
    chrome_options.add_argument('--headless=new') 
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument(
        "user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.90 Safari/537.36"
    )

    driver = webdriver.Chrome(
        #service=Service(ChromeDriverManager().install()), options=chrome_options
        service=Service("/usr/local/bin/chromedriver"), options=chrome_options
    )

    # 올리브영 스킨케어 랭킹 페이지 열기
    url = "https://www.oliveyoung.co.kr/store/main/getBestList.do?dispCatNo=900000100100001&fltDispCatNo=10000010001&pageIdx=1&rowsPerPage=8&t_page=%EB%9E%AD%ED%82%B9&t_click=%ED%8C%90%EB%A7%A4%EB%9E%AD%ED%82%B9_%EC%8A%A4%ED%82%A8%EC%BC%80%EC%96%B4"
    driver.get(url)

    # 페이지 로딩 대기
    time.sleep(2.5)

    data = []
    goods_no_list = []
    items = driver.find_elements(
        By.CSS_SELECTOR, "div.TabsConts.on ul.cate_prd_list li"
    )
    log.info(f"[get_top100_skincare] 상품 개수: {len(items)}")
    rank = 1
    # 타임스탬프 생성
    collected_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for item in items:
        try:
            # 순위 (.thumb_flag 요소가 없는 경우 대비)
            try:
                rank_tag = item.find_element(By.CSS_SELECTOR, ".thumb_flag")
                if rank_tag.text.strip().isdigit():
                    rank_val = int(rank_tag.text.strip())
                    rank = rank_val
                else:
                    rank_val = rank
            except Exception:
                rank_val = rank

            try:
                # 브랜드
                brand = item.find_element(By.CSS_SELECTOR, ".tx_brand").text.strip()
                # 제품명
                name = item.find_element(By.CSS_SELECTOR, ".tx_name").text.strip()
                # 제품 코드 (goodsNo)
                try:
                    a_tag = item.find_element(By.CSS_SELECTOR, "a[data-ref-goodsno]")
                    goods_no = a_tag.get_attribute("data-ref-goodsno")
                except Exception:
                    goods_no = ""
                # goods_no_list에 바로 추가
                if goods_no:
                    goods_no_list.append(goods_no)
                
                # 구매가격
                try:
                    price_final = item.find_element(
                        By.CSS_SELECTOR, ".prd_price .tx_cur .tx_num"
                    ).text.strip()
                except Exception as e:
                    log.warning(f"[get_top100_skincare] 구매가격 정보 파싱 실패: {e}")
                    price_final = ""
                # 정가 (null 허용)
                try:
                    price_original = item.find_element(
                        By.CSS_SELECTOR, ".prd_price .tx_org .tx_num"
                    ).text.strip()
                except Exception:
                    price_original = price_final
                # 기타 프로모션 정보(null 허용)
                try:
                    flag_spans = item.find_elements(By.CSS_SELECTOR, ".prd_flag .icon_flag")
                    flag_list = [
                        span.text.strip() for span in flag_spans if span.text.strip()
                    ]
                except Exception:
                    flag_list = []
            except Exception as e:
                log.warning(f"[get_top100_skincare] 제품 정보 파싱 실패: {e}")
                continue

            # 올리브영 PB 브랜드 여부 확인
            pb_brands = [
                "바이오힐 보",
                "브링그린",
                "웨이크메이크",
                "컬러그램",
                "필리밀리",
                "아이디얼포맨",
                "라운드어라운드",
                "식물나라",
                "케어플러스",
                "탄탄",
                "딜라이트 프로젝트",
            ]
            is_pb = 1 if brand in pb_brands else 0

            # 일시품절 여부 확인
            try:
                soldout_flag = item.find_element(By.CSS_SELECTOR, "span.status_flag.soldout")
                is_soldout = soldout_flag.is_displayed()
            except Exception:
                is_soldout = False

            data.append(
                {
                    "rank": rank_val,
                    "brandName": brand,
                    "isPb": is_pb,
                    "goodsName": name,
                    "salePrice": price_final,
                    "originalPrice": price_original,
                    "flagList": flag_list,  # 리스트로 저장
                    "createdAt": collected_at,
                    "isSoldout": bool(is_soldout),
                    "category": "스킨케어"
                }
            )
            log.info(f"[get_top100_skincare] {rank_val}위 상품: {brand} {name} (goods_no: {goods_no})")
            rank += 1

        except Exception as e:
            log.warning(f"[get_top100_skincare] 제품 정보 파싱 실패: {e}")
            continue

    driver.quit()
    log.info(f"[get_top100_skincare] 크롤링 종료: 총 상품 {len(data)}개, goods_no {len(goods_no_list)}개")
    return data, goods_no_list


def get_product_detail_info(sb, goods_no: str) -> dict:
    log = LoggingMixin().log
    url = f"https://www.oliveyoung.co.kr/store/goods/getGoodsDetail.do?goodsNo={goods_no}"
    log.info(f"[get_product_detail_info] 시작: goods_no={goods_no}")
    #sb.uc_open_with_reconnect(url, reconnect_time=5)  # 속도 더 느림
    sb.open(url)
    time.sleep(1)
    html = sb.driver.page_source
    soup = BeautifulSoup(html, 'html.parser')

    try:
        review_info = soup.select_one("#repReview em")
        total_review = int(review_info.text.strip().replace("(", "").replace("건)", "").replace(",", ""))
        log.info(f"[get_product_detail_info] 총 리뷰수: {total_review}")
    except Exception as e:
        log.warning(f"[get_product_detail_info] 총 리뷰수 파싱 실패: {e}")
        total_review = 0

    try:
        review_score = soup.select_one("#repReview b")
        review_score = float(review_score.text.strip())
        log.info(f"[get_product_detail_info] 리뷰평점: {review_score}")
    except Exception as e:
        log.warning(f"[get_product_detail_info] 리뷰평점 파싱 실패: {e}")
        review_score = None

    # 리뷰 분포 기본값
    pctOf5 = pctOf4 = pctOf3 = pctOf2 = pctOf1 = None

    # 리뷰가 1건 이상 있을 때만 리뷰탭 클릭 및 분포 수집
    total_comment = ""
    if total_review > 0:
        try:
            sb.click("a.goods_reputation")
            WebDriverWait(sb.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "ul.graph_list span.per"))
            )
            percent_elements = sb.find_elements("css selector", "ul.graph_list span.per")
            percent_list = [el.text.strip() for el in percent_elements]
            if len(percent_list) == 5:
                pctOf5 = percent_list[0]
                pctOf4 = percent_list[1]
                pctOf3 = percent_list[2]
                pctOf2 = percent_list[3]
                pctOf1 = percent_list[4]
                log.info(f"[get_product_detail_info] 리뷰 분포: {percent_list}")

            try:
                # 대표 코멘트 추출
                comment_tag = sb.find_element(By.CSS_SELECTOR, "p.img_face em")
                total_comment = comment_tag.text.strip() if comment_tag else ""
                log.info(f"[get_product_detail_info] 대표 코멘트 추출: {total_comment}")
            except Exception:
                total_comment = ""
                log.warning("[get_product_detail_info] 대표 코멘트 추출 실패")
        except Exception as e:
            log.warning(f"[get_product_detail_info] 리뷰 정보 수집 실패: {e}")
    else:
        log.warning("[get_product_detail_info] 리뷰 정보 없음: 리뷰 수가 0건 입니다.")

    # === 상세스펙(구매정보) 추출 ===
    # 구매정보 탭 클릭
    try:
        sb.click("a.goods_buyinfo")
        time.sleep(1)  # ajax 로딩 대기
        html = sb.driver.page_source
        soup = BeautifulSoup(html, 'html.parser')
        log.info("[get_product_detail_info] 구매정보 탭 클릭 및 파싱 성공")
    except Exception as e:
        log.warning(f"[get_product_detail_info] 구매정보 탭 클릭 실패: {e}")

    # 용량, 주요사양, 성분 추출
    def get_detail_info(soup, title):
        try:
            dl_tags = soup.select("div#artcInfo dl.detail_info_list")
            for dl in dl_tags:
                dt = dl.select_one("dt")
                dd = dl.select_one("dd")
                if dt and dd:
                    dt_text = dt.text.strip()
                    dd_text = dd.text.strip()
                    if title in dt_text:
                        log.info(f"[get_product_detail_info] {title} 추출 성공!")
                        return dd_text
        except Exception as e:
            log.warning(f"[get_product_detail_info] 상세 정보 파싱 실패 ({title}): {e}")
        return ""

    # === reviewDetail 파싱 ===
    review_detail = []
    try:
        poll_div = soup.select_one("div.poll_all")
        if poll_div:
            # 우선 dl.poll_type2.type3을 찾고, 없으면 dl.poll_type2만 찾기
            dl_tags = poll_div.select("dl.poll_type2.type3")
            if not dl_tags:
                dl_tags = poll_div.select("dl.poll_type2")
            for dl in dl_tags:
                type_name = dl.select_one("dt span")
                type_name = type_name.text.strip() if type_name else ""
                for li in dl.select("dd ul.list > li"):
                    value = li.select_one("span.txt")
                    value = value.text.strip() if value else ""
                    gauge = li.select_one("em.per")
                    gauge = gauge.text.strip() if gauge else ""
                    review_detail.append({
                        "gauge": gauge,
                        "type": type_name,
                        "value": value
                    })
            log.info(f"[get_product_detail_info] reviewDetail 파싱 성공: {review_detail}")
    except Exception as e:
        log.warning(f"[get_product_detail_info] reviewDetail 파싱 실패: {e}")

    # 상세스펙 정보 추출
    detail_spec = {}
    spec_map = {
        "용량": "capacity",
        "주요 사양": "detail",
        "성분": "ingredient"
    }
    for title, key in spec_map.items():
        detail_spec[key] = get_detail_info(soup, title)

    return {
        "totalComment": total_comment,
        "numOfReviews": total_review,
        "avgReview": review_score,
        "pctOf5": pctOf5,
        "pctOf4": pctOf4,
        "pctOf3": pctOf3,
        "pctOf2": pctOf2,
        "pctOf1": pctOf1,
        "reviewDetail": review_detail,
        **detail_spec,

    }
