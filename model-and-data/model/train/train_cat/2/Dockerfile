FROM pytorch/pytorch:2.6.0-cuda12.4-cudnn9-devel

RUN pip install --no-cache-dir transformers[torch]>=4.45.0 accelerate peft scikit-learn pandas numpy safetensors nltk pymorphy3 stanza spacy nlpaug dask[complete] sentencepiece sacremoses

RUN python -m spacy download ru_core_news_sm

RUN python -c "import nltk; nltk.download('stopwords'); nltk.download('punkt_tab'); nltk.download('punkt')"

WORKDIR /app

COPY train_cat.py /app/train_cat.py
COPY preprocess_single_review.py /app/preprocess_single_review.py
COPY preprocess_common.py /app/preprocess_common.py

CMD ["python", "train_cat.py"]