from seleniumbase import SB
from bs4 import BeautifulSoup
import pandas as pd
import datetime
import time
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import logging
import json
import os

# 환경 변수
os.environ["UC_DRIVER_PATH"] = "/opt/airflow/uc_driver"

def get_brand(brand_name, brand_code) -> pd.DataFrame:
    url = f"https://www.oliveyoung.co.kr/store/display/getBrandShopDetail.do?onlBrndCd={brand_code}"
    data = []
    collected_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with SB(uc=True, test=True, headless=True) as sb:
        sb.uc_open_with_reconnect(url, reconnect_time=20)

        page = 1
        while True:
            if page > 1:
                try:
                    # 페이지네이션 버튼 클릭 (페이지가 없으면 break)
                    sb.click(f"div.pageing a[data-page-no='{page}']")
                    time.sleep(2)  # ajax 로딩 대기
                except Exception as e:
                    print(f"{page}페이지 버튼 클릭 실패 또는 더 이상 페이지 없음: {e}")
                    break

            html = sb.driver.page_source
            soup = BeautifulSoup(html, "html.parser")

            # 브랜드명 추출
            try:
                brand = soup.select_one("h2.title-detail-brand").text.strip()
            except Exception:
                brand = brand_name

            # 상품 목록 추출
            items = soup.select("ul.prod-list.goodsProd > li")
            if not items:
                print(f"{page}페이지에 상품이 없습니다.")
                break

            for item in items:
                is_pb = 1
                try:
                    name = item.select_one("span.prod-name.double-line").text.strip()
                except Exception:
                    name = ""
                try:
                    a_tag = item.select_one("a[data-ref-goodsno]")
                    goods_no = a_tag["data-ref-goodsno"] if a_tag else ""
                except Exception:
                    goods_no = ""
                try:
                    price_final = item.select_one("strong.total").text.strip().replace("원", "").replace(",", "").replace("~", "")
                except Exception:
                    price_final = ""
                try:
                    price_original = item.select_one("span.origin").text.strip().replace("원", "").replace(",", "")
                except Exception:
                    price_original = ""
                try:
                    flag_spans = item.select("div.flags span.flag")
                    flag_list = [span.text.strip() for span in flag_spans if span.text.strip()]
                    flag_str = ",".join(flag_list) if flag_list else ""
                except Exception:
                    flag_str = ""
                try:
                    soldout_flag = item.select_one("span.status_flag.soldout")
                    is_soldout = bool(soldout_flag)
                except Exception:
                    is_soldout = False

                data.append({
                    "brandName": brand,
                    "isPB": is_pb,
                    "goodsName": name,
                    "goodsNo": goods_no,
                    "salePrice": price_final,
                    "originalPrice": price_original,
                    "flagList": flag_str,
                    "isSoldout": is_soldout,
                    "createdAt": collected_at
                })
            # 다음 페이지로
            page += 1

    return pd.DataFrame(data)

def get_product_detail_info(sb, goods_no: str) -> dict:
    url = f"https://www.oliveyoung.co.kr/store/goods/getGoodsDetail.do?goodsNo={goods_no}"
    sb.uc_open_with_reconnect(url, reconnect_time=5)
    time.sleep(1)
    html = sb.driver.page_source
    soup = BeautifulSoup(html, 'html.parser')

    # 카테고리 추출
    try:
        category = soup.select_one("a.cate_y#midCatNm").text.strip()
    except Exception as e:
        print(f"카테고리 추출 실패: {e}")
        category = ""

    # 대표 코멘트
    try:
        comment_tag = soup.select_one("p.img_face em")
        total_comment = comment_tag.text.strip() if comment_tag else ""
    except Exception as e:
        print(f"대표 코멘트 파싱 실패: {e}")
        total_comment = ""
    
    # 총리뷰수
    try:
        review_info = soup.select_one("#repReview em")
        total_review = int(review_info.text.strip().replace("(", "").replace("건)", "").replace(",", ""))
    except Exception as e:
        print(f"총 리뷰수 파싱 실패: {e}")
        total_review = 0

    # 리뷰평점
    try:
        review_score = soup.select_one("#repReview b")
        review_score = float(review_score.text.strip())
    except Exception as e:
        print(f"리뷰평점 파싱 실패: {e}")
        review_score = None

    # 리뷰 분포 기본값
    pctOf5 = pctOf4 = pctOf3 = pctOf2 = pctOf1 = None
    review_detail = ""

    # 리뷰가 1건 이상 있을 때만 리뷰탭 클릭 및 분포 수집
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
                    print(f"리뷰 설문 수집 오류: {e}")
            review_detail = json.dumps(review_detail, ensure_ascii=False)

        except Exception as e:
            print("리뷰 정보 없음:", e)

    # === 상세스펙(구매정보) 추출 ===
    # 구매정보 탭 클릭
    try:
        sb.click("a.goods_buyinfo")
        time.sleep(1)  # ajax 로딩 대기
        html = sb.driver.page_source
        soup = BeautifulSoup(html, 'html.parser')
    except Exception as e:
        print("구매정보 탭 클릭 실패:", e)

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
        print(f"[상세 스펙 파싱 오류]: {e}")

    return {
        "category": category,
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

##### 실행 코드 #####
PB_BRAND_CODE_DICT = {
    "바이오힐 보": "A000897",
    "브링그린": "A002253",
    "웨이크메이크": "A001240",
    "컬러그램": "A002712",
    "필리밀리": "A002502",
    "아이디얼포맨": "A001643",
    "라운드어라운드": "A001306",
    "식물나라": "A000036",
    "케어플러스": "A003339",
    "탄탄": "A015673",
    "딜라이트 프로젝트": "A003361",
}

# for brand_name, brand_code in PB_BRAND_CODE_DICT.items():
# df = get_brand("A000036")
# 
# with SB(uc=True, test=True) as sb:
#     detail_list = []
#     for goods_no in df['goodsNo']:
#         detail = get_product_detail_info(sb, goods_no)
#         detail_list.append(detail)
# detail_df = pd.DataFrame(detail_list)
# result_df = pd.concat([df.reset_index(drop=True), detail_df.reset_index(drop=True)], axis=1)
# 
# now_str = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
# result_df.to_json(f'suncare_result_{now_str}.json', orient='records', force_ascii=False, indent=2)