FROM --platform=linux/amd64 apache/airflow:3.0.2

USER airflow

# ✅ Airflow 2.9.1 삭제
RUN pip uninstall -y apache-airflow

# ✅ 2.10.1 설치 (constraints 사용)
RUN pip install apache-airflow==2.10.1 \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.1/constraints-3.12.txt"

# 루트 권한 전환 (시스템 패키지 설치를 위해)
USER root

# ✅ 3. 시스템 패키지 설치 (Chrome 실행 위한 라이브러리)
RUN rm -f /etc/apt/sources.list.d/google-chrome.list || true && \
    apt-get update && apt-get install -y \
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
COPY google-chrome-stable_114.0.5735.90-1_amd64.deb /tmp/
RUN dpkg -i /tmp/google-chrome-stable_114.0.5735.90-1_amd64.deb || true && \
    apt-get install -f -y && \
    rm /tmp/google-chrome-stable_114.0.5735.90-1_amd64.deb

# ChromeDriver 설치
RUN wget https://chromedriver.storage.googleapis.com/114.0.5735.90/chromedriver_linux64.zip && \
    unzip -o chromedriver_linux64.zip && \
    mv chromedriver /usr/local/bin/ && \
    rm chromedriver_linux64.zip

# airflow 사용자로 돌아가기
USER airflow

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
