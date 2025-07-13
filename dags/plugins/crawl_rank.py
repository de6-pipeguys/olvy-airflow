from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import logging
import time
import json
import datetime
from bs4 import BeautifulSoup

# 환경변수 설정 (ARM64 아키텍처용 드라이버 다운로드 및 uc 관련 권한 경로 지정)
import os
os.environ['WDM_ARCH'] = 'arm64'
os.environ["UC_DRIVER_PATH"] = "/opt/airflow/uc_driver"

def get_top100(url: str) -> tuple:
    chrome_options = Options()
    chrome_options.add_argument('--headless=new') 
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

    driver.get(url)

    # 페이지 로딩 대기
    time.sleep(2.5)
    
    data = []
    goods_no_list = []
    items = driver.find_elements(
        By.CSS_SELECTOR, "div.TabsConts.on ul.cate_prd_list li"
    )
    print(f"📦 상품 수집 시도: {len(items)}개")
    
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
                # 정가 (null 허용)
                try:
                    price_original = item.find_element(
                        By.CSS_SELECTOR, ".prd_price .tx_org .tx_num"
                    ).text.strip()
                except Exception:
                    price_original = ""
                # 구매가격
                try:
                    price_final = item.find_element(
                        By.CSS_SELECTOR, ".prd_price .tx_cur .tx_num"
                    ).text.strip()
                except Exception as e:
                    print(f"구매가격 정보 파싱 실패: {e}")
                    price_final = ""
                # 기타 프로모션 정보(null 허용)
                try:
                    flag_spans = item.find_elements(By.CSS_SELECTOR, ".prd_flag .icon_flag")
                    flag_list = [
                        span.text.strip() for span in flag_spans if span.text.strip()
                    ]
                except Exception:
                    flag_list = []
            except Exception as e:
                print(f"제품 정보 파싱 실패: {e}")
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
                    "isSoldout": bool(is_soldout)
                }
            )
            rank += 1

        except Exception as e:
            print(f"제품 정보 파싱 실패: {e}")
            continue

    driver.quit()

    return data, goods_no_list


def get_rank_detail_info(sb, goods_no: str) -> dict:
    url = f"https://www.oliveyoung.co.kr/store/goods/getGoodsDetail.do?goodsNo={goods_no}"
    sb.uc_open_with_reconnect(url, reconnect_time=5)
    time.sleep(1)
    html = sb.driver.page_source
    soup = BeautifulSoup(html, 'html.parser')

    # 카테고리 추출
    # try:
    #     category = soup.select_one("a.cate_y#midCatNm").text.strip()
    #     logging.info(f"카테고리: {category}")
    # except Exception as e:
    #     logging.warning(f"카테고리 추출 실패: {e}")
    #     category = ""
    
    # 총리뷰수
    try:
        review_info = soup.select_one("#repReview em")
        total_review = int(review_info.text.strip().replace("(", "").replace("건)", "").replace(",", ""))
        logging.info(f"총리뷰수: {total_review}")
    except Exception as e:
        logging.warning(f"총 리뷰수 파싱 실패: {e}")
        total_review = 0

    # 리뷰평점
    try:
        review_score = soup.select_one("#repReview b")
        review_score = float(review_score.text.strip())
        logging.info(f"리뷰평점: {review_score}")
    except Exception as e:
        logging.warning(f"리뷰평점 파싱 실패: {e}")
        review_score = None

    # 리뷰 분포 기본값
    pctOf5 = pctOf4 = pctOf3 = pctOf2 = pctOf1 = None
    review_detail = ""

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
                logging.info(f"리뷰 분포: {percent_list}")

            try:
                # 대표 코멘트 추출
                comment_tag = sb.find_element(By.CSS_SELECTOR, "p.img_face em")
                total_comment = comment_tag.text.strip() if comment_tag else ""
                logging.info(f"[get_product_detail_info] 대표 코멘트 추출: {total_comment}")
            except Exception:
                total_comment = ""
                logging.warning("[get_product_detail_info] 대표 코멘트 추출 실패")

            # reviewDetail 정보
            review_detail = []
            polls = sb.find_elements("css selector", "dl.poll_type2.type3")
            for poll in polls:
                try:
                    title = poll.find_element("css selector", "span").text.strip()
                    li_tags = poll.find_elements("css selector", "ul.list > li")
                    for li in li_tags:
                        label = li.find_element("css selector", "span.txt").text.strip()
                        percent = li.find_element("css selector", "em.per").text.strip()
                        review_detail.append({
                            "type": title,
                            "value": label,
                            "gauge": percent
                        })
                except Exception as e:
                    logging.warning(f"리뷰 설문 수집 오류: {e}")
            review_detail = json.dumps(review_detail, ensure_ascii=False)

        except Exception as e:
            logging.warning("리뷰 정보 없음:", e)

    # === 상세스펙(구매정보) 추출 ===
    # 구매정보 탭 클릭
    try:
        sb.click("a.goods_buyinfo")
        time.sleep(1)  # ajax 로딩 대기
        html = sb.driver.page_source
        soup = BeautifulSoup(html, 'html.parser')
    except Exception as e:
        logging.warning("구매정보 탭 클릭 실패:", e)

    # === 한글 키 → 영어 키 매핑 ===
    title_map = {
        "용량": "capacity",
        "주요 사양": "detail",
        "모든 성분": "ingredient"
    }

    # 기본값 세팅
    detail_spec = {
        "capacity": "",
        "detail": "",
        "ingredient": ""
    }

    try:
        dl_tags = soup.select("div#artcInfo dl.detail_info_list")
        for dl in dl_tags:
            dt = dl.select_one("dt")
            dd = dl.select_one("dd")
            if dt and dd:
                dt_text = dt.text.strip()
                dd_text = dd.text.strip()

                for kr_title, en_key in title_map.items():
                    if kr_title in dt_text:
                        detail_spec[en_key] = dd_text
    except Exception as e:
        logging.warning(f"[상세 스펙 파싱 오류]: {e}")

    return {
        # "category": category,
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