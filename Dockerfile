FROM python:3.7

COPY requirements.in .
RUN python3 -m pip install --no-cache -rrequirements.in

COPY achternamen.py .

CMD python3 -m achternamen
