FROM apify/actor-python:3.11

# Node.js 설치 (X-Bogus 서명 생성용 - xbogus.js 실행에 필요)
RUN apt-get update && apt-get install -y --no-install-recommends nodejs && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/

CMD ["python", "src/main.py"]
