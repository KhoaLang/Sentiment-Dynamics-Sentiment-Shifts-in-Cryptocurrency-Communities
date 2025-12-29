from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch

LABELS = ["anger", "disgust", "fear", "joy", "sadness", "surprise"]


MODEL_NAME = "j-hartmann/emotion-english-distilroberta-base"
tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
model = AutoModelForSequenceClassification.from_pretrained(MODEL_NAME)
model.eval()

def score_emotions(texts):
    inputs = tokenizer(texts, return_tensors="pt", truncation=True, padding=True)
    with torch.no_grad():
        logits = model(**inputs).logits
    probs = torch.softmax(logits, dim=1).numpy()
    return probs

pdf = {}

scores = score_emotions(["Most people think bull market is over but Bitamine keeps buying , adding $320 million to ETH Treasury. What am I missing?"])
for i, label in enumerate(LABELS):
    pdf[label] = scores[:, i]
print(pdf)