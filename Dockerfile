FROM python:3.7

WORKDIR /usr/local/bin

COPY /src/requirements.txt .
COPY /src/sendtweet.py .
COPY /src/settings.py .

RUN cat requirements.txt | xargs -n 1 python3 -m pip install

CMD python3 sendtweet.py