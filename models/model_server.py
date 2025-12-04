#!/usr/bin/env python3
"""
Model serving API using FastAPI.
Provides endpoints for sentiment analysis and other ML models.
"""

import os
import sys
import logging
import joblib
import numpy as np
import pandas as pd
from typing import Dict, Any, List, Optional
from datetime import datetime

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import uvicorn
from transformers import pipeline
import torch

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Pydantic models
class SentimentRequest(BaseModel):
    text: str = Field(..., description="Text to analyze")
    model_type: str = Field("general", description="Model type: general, crypto, financial")
    language: str = Field("en", description="Language code")


class SentimentResponse(BaseModel):
    label: str = Field(..., description="Sentiment label")
    score: float = Field(..., description="Sentiment score")
    confidence: float = Field(..., description="Confidence score")
    probabilities: Dict[str, float] = Field(..., description="Class probabilities")
    processing_time_ms: float = Field(..., description="Processing time in milliseconds")


class BatchSentimentRequest(BaseModel):
    texts: List[str] = Field(..., description="List of texts to analyze")
    model_type: str = Field("general", description="Model type")
    language: str = Field("en", description="Language code")


class ModelInfo(BaseModel):
    model_type: str
    model_name: str
    version: str
    supported_languages: List[str]
    loaded_at: datetime
    inference_time_ms: float


class ModelServer:
    """FastAPI model server for sentiment analysis."""

    def __init__(self):
        """Initialize model server."""
        self.app = FastAPI(
            title="Sentiment Analysis Model Server",
            description="ML model serving for real-time sentiment analysis",
            version="1.0.0"
        )

        # Setup CORS
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

        # Initialize models
        self.models = {}
        self.load_models()

        # Setup routes
        self._setup_routes()

    def load_models(self):
        """Load all ML models."""
        logger.info("Loading ML models...")

        try:
            # Load general sentiment model
            self.models['general'] = self._load_general_model()
            logger.info("General sentiment model loaded")

            # Load crypto-specific model
            self.models['crypto'] = self._load_crypto_model()
            logger.info("Crypto sentiment model loaded")

            # Load financial sentiment model
            self.models['financial'] = self._load_financial_model()
            logger.info("Financial sentiment model loaded")

            logger.info("All models loaded successfully")

        except Exception as e:
            logger.error(f"Error loading models: {e}")
            raise

    def _load_general_model(self):
        """Load general sentiment model."""
        model_path = os.getenv('GENERAL_SENTIMENT_MODEL_PATH', 'models/general_sentiment_model.joblib')

        try:
            if os.path.exists(model_path):
                model = joblib.load(model_path)
                logger.info(f"Loaded scikit-learn model from {model_path}")
                return {
                    'type': 'sklearn',
                    'model': model,
                    'name': 'General Sentiment Model',
                    'version': '1.0'
                }
            else:
                # Use HuggingFace model as fallback
                logger.info("Using HuggingFace transformer model")
                model = pipeline(
                    "sentiment-analysis",
                    model="cardiffnlp/twitter-roberta-base-sentiment-latest",
                    device=0 if torch.cuda.is_available() else -1
                )
                return {
                    'type': 'transformer',
                    'model': model,
                    'name': 'Twitter RoBERTa Sentiment',
                    'version': 'latest'
                }
        except Exception as e:
            logger.error(f"Error loading general model: {e}")
            return None

    def _load_crypto_model(self):
        """Load cryptocurrency-specific sentiment model."""
        model_path = os.getenv('CRYPTO_SENTIMENT_MODEL_PATH', 'models/crypto_sentiment_model.joblib')

        try:
            if os.path.exists(model_path):
                model = joblib.load(model_path)
                return {
                    'type': 'sklearn',
                    'model': model,
                    'name': 'Crypto Sentiment Model',
                    'version': '1.0'
                }
            else:
                logger.info("Crypto model not found, will use general model")
                return self._load_general_model()
        except Exception as e:
            logger.error(f"Error loading crypto model: {e}")
            return None

    def _load_financial_model(self):
        """Load financial sentiment model."""
        model_path = os.getenv('FINANCIAL_SENTIMENT_MODEL_PATH', 'models/financial_sentiment_model.joblib')

        try:
            if os.path.exists(model_path):
                model = joblib.load(model_path)
                return {
                    'type': 'sklearn',
                    'model': model,
                    'name': 'Financial Sentiment Model',
                    'version': '1.0'
                }
            else:
                logger.info("Financial model not found, will use general model")
                return self._load_general_model()
        except Exception as e:
            logger.error(f"Error loading financial model: {e}")
            return None

    def _setup_routes(self):
        """Setup API routes."""

        @self.app.post("/predict/sentiment", response_model=SentimentResponse)
        async def predict_sentiment(request: SentimentRequest):
            """Predict sentiment for a single text."""
            start_time = datetime.now()

            try:
                # Validate input
                if not request.text or len(request.text.strip()) == 0:
                    raise HTTPException(status_code=400, detail="Text cannot be empty")

                if len(request.text) > 10000:
                    raise HTTPException(status_code=400, detail="Text too long (max 10000 characters)")

                # Get appropriate model
                model_info = self.models.get(request.model_type, self.models['general'])
                if not model_info:
                    raise HTTPException(status_code=404, detail="Model not found")

                # Make prediction
                result = self._predict_with_model(request.text, model_info)

                # Calculate processing time
                processing_time = (datetime.now() - start_time).total_seconds() * 1000

                return SentimentResponse(
                    label=result['label'],
                    score=result['score'],
                    confidence=result['confidence'],
                    probabilities=result['probabilities'],
                    processing_time_ms=processing_time
                )

            except HTTPException:
                raise
            except Exception as e:
                logger.error(f"Error in sentiment prediction: {e}")
                raise HTTPException(status_code=500, detail="Prediction failed")

        @self.app.post("/predict/batch-sentiment")
        async def predict_batch_sentiment(request: BatchSentimentRequest):
            """Predict sentiment for multiple texts."""
            try:
                # Validate input
                if not request.texts:
                    raise HTTPException(status_code=400, detail="Texts list cannot be empty")

                if len(request.texts) > 1000:
                    raise HTTPException(status_code=400, detail="Too many texts (max 1000)")

                # Get model
                model_info = self.models.get(request.model_type, self.models['general'])
                if not model_info:
                    raise HTTPException(status_code=404, detail="Model not found")

                # Process batch
                results = []
                for text in request.texts:
                    try:
                        result = self._predict_with_model(text, model_info)
                        results.append({
                            'text': text[:100] + '...' if len(text) > 100 else text,
                            'prediction': result
                        })
                    except Exception as e:
                        results.append({
                            'text': text[:100] + '...' if len(text) > 100 else text,
                            'error': str(e)
                        })

                return {
                    'model_type': request.model_type,
                    'total_texts': len(request.texts),
                    'successful_predictions': sum(1 for r in results if 'prediction' in r),
                    'results': results
                }

            except HTTPException:
                raise
            except Exception as e:
                logger.error(f"Error in batch sentiment prediction: {e}")
                raise HTTPException(status_code=500, detail="Batch prediction failed")

        @self.app.get("/models", response_model=List[ModelInfo])
        async def list_models():
            """List available models."""
            models_info = []
            for model_type, model_info in self.models.items():
                if model_info:
                    models_info.append(ModelInfo(
                        model_type=model_type,
                        model_name=model_info['name'],
                        version=model_info['version'],
                        supported_languages=['en', 'es', 'fr', 'de'],
                        loaded_at=datetime.now(),
                        inference_time_ms=50.0  # Placeholder
                    ))
            return models_info

        @self.app.get("/models/{model_type}")
        async def get_model_info(model_type: str):
            """Get information about a specific model."""
            model_info = self.models.get(model_type)
            if not model_info:
                raise HTTPException(status_code=404, detail="Model not found")

            return {
                'model_type': model_type,
                'name': model_info['name'],
                'version': model_info['version'],
                'type': model_info['type'],
                'loaded_at': datetime.now().isoformat()
            }

        @self.app.get("/health")
        async def health_check():
            """Health check endpoint."""
            return {
                'status': 'healthy',
                'timestamp': datetime.now().isoformat(),
                'models_loaded': len(self.models),
                'model_types': list(self.models.keys())
            }

        @self.app.get("/")
        async def root():
            """Root endpoint."""
            return {
                'service': 'Sentiment Analysis Model Server',
                'version': '1.0.0',
                'endpoints': [
                    '/predict/sentiment',
                    '/predict/batch-sentiment',
                    '/models',
                    '/health'
                ]
            }

    def _predict_with_model(self, text: str, model_info: Dict[str, Any]) -> Dict[str, Any]:
        """Make prediction using specified model."""
        model = model_info['model']
        model_type = model_info['type']

        if model_type == 'sklearn':
            # Scikit-learn model
            prediction = model.predict([text])[0]
            probabilities = model.predict_proba([text])[0]

            label_map = {0: 'negative', 1: 'neutral', 2: 'positive'}
            label = label_map.get(prediction, 'neutral')

            return {
                'label': label,
                'score': float(probabilities[2] - probabilities[0]),  # pos - neg
                'confidence': float(max(probabilities)),
                'probabilities': {
                    'positive': float(probabilities[2]),
                    'negative': float(probabilities[0]),
                    'neutral': float(probabilities[1])
                }
            }

        elif model_type == 'transformer':
            # HuggingFace transformer
            result = model(text)[0]

            # Normalize labels
            label = result['label'].lower()
            if 'negative' in label:
                label = 'negative'
                score = -result['score']
            elif 'positive' in label:
                label = 'positive'
                score = result['score']
            else:
                label = 'neutral'
                score = 0.0

            return {
                'label': label,
                'score': score,
                'confidence': abs(result['score']),
                'probabilities': {
                    'positive': float(result['score']) if label == 'positive' else 0.0,
                    'negative': float(-result['score']) if label == 'negative' else 0.0,
                    'neutral': 0.5
                }
            }

        else:
            raise ValueError(f"Unknown model type: {model_type}")

    def start(self, host: str = "0.0.0.0", port: int = 8000):
        """Start the model server."""
        logger.info(f"Starting model server on {host}:{port}")
        uvicorn.run(self.app, host=host, port=port)


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description='Sentiment Analysis Model Server')
    parser.add_argument('--host', type=str, default='0.0.0.0', help='Host to bind to')
    parser.add_argument('--port', type=int, default=8000, help='Port to bind to')
    args = parser.parse_args()

    server = ModelServer()
    server.start(host=args.host, port=args.port)


if __name__ == '__main__':
    main()