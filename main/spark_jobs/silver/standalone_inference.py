from transformers import pipeline
import torch


device = 0 if torch.cuda.is_available() else -1

sentiment_model = pipeline("sentiment-analysis", model="siebert/sentiment-roberta-large-english", device=device)

print(sentiment_model("Another ‘bull market’ where retail buys the top and whales disappear—classic."))
