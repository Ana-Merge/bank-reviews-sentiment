FROM pytorch/pytorch:2.6.0-cpu-py310

RUN pip install --no-cache-dir \
    transformers>=4.45.0 \
    scikit-learn \
    pandas \
    numpy \
    safetensors \
    nltk \
    pymorphy3 \
    fastapi \
    uvicorn

WORKDIR /app

COPY predict.py /app/
COPY preprocess.py /app/
COPY ./results/topic_model /app/topic_model
COPY ./results/sentiment_model /app/sentiment_model
COPY ./results/logs /app/logs

EXPOSE 8000

CMD ["uvicorn", "predict:app", "--host", "0.0.0.0", "--port", "8000"]