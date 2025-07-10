FROM --platform=linux/amd64 apache/airflow:2.9.1

# Chrome 설치는 루트 권한이 필요하므로
USER root

# 필수 패키지 설치
RUN apt-get update && apt-get install -y \
    wget \
    gnupg2 \
    curl \
    unzip \
    fonts-liberation \
    libappindicator3-1 \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libcups2 \
    libdbus-1-3 \
    libgdk-pixbuf2.0-0 \
    libnspr4 \
    libnss3 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    xdg-utils \
    --no-install-recommends

# Chrome 설치
RUN wget https://mirror.cs.uchicago.edu/google-chrome/pool/main/g/google-chrome-stable/google-chrome-stable_114.0.5735.90-1_amd64.deb && \
    dpkg -i google-chrome-stable_114.0.5735.90-1_amd64.deb || true && \
    apt-get install -f -y && \
    rm google-chrome-stable_114.0.5735.90-1_amd64.deb

# ChromeDriver 설치
RUN wget https://chromedriver.storage.googleapis.com/114.0.5735.90/chromedriver_linux64.zip && \
    unzip chromedriver_linux64.zip && \
    mv chromedriver /usr/local/bin/ && \
    rm chromedriver_linux64.zip

# airflow 유저로 전환 pip install
USER airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 권한 설정 위해 root로 다시 전환
USER root

# airflow 사용자가 sb.uc_open_with_reconnect 사용할 수 있도록 권한 설정
# RUN mkdir -p /home/airflow/.local/lib/python3.12/site-packages/seleniumbase/drivers && \
#     chown -R airflow /home/airflow/.local/lib/python3.12/site-packages/seleniumbase/drivers && \
#     chmod -R 755 /home/airflow/.local/lib/python3.12/site-packages/seleniumbase/drivers

# ✅ UC_DRIVER_PATH 지정 경로 생성 + 권한 부여
RUN mkdir -p /opt/airflow/uc_driver && \
    chmod -R 777 /opt/airflow/uc_driver

# ✅ fallback 경로도 강제로 생성 + 권한 부여 (SeleniumBase 내부 fallback 방지용)
RUN mkdir -p /home/airflow/.local/lib/python3.12/site-packages/seleniumbase/drivers && \
    chmod -R 777 /home/airflow/.local/lib/python3.12/site-packages/seleniumbase/drivers

    # ✅ 전역 환경변수 설정 추가
ENV UC_DRIVER_PATH=/opt/airflow/uc_driver

# airflow 유저로 다시 전환
USER airflow

# COPY requirements.txt .
# RUN pip install --no-cache-dir -r requirements.txt