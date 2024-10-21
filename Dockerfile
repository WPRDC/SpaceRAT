FROM python:3
LABEL authors="Steve Saylor"

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
RUN pip install -e .

ENTRYPOINT ["flask", "--app", "spacerat.api", "run", "--host=0.0.0.0"]