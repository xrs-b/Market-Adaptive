from __future__ import annotations

import json
import pickle
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import math
import pandas as pd

FEATURE_COLUMNS = [
    'RSI', 'MACD', 'MACD_signal', 'MACD_hist',
    'BB_upper', 'BB_lower', 'BB_position',
    'MA5_ratio', 'trend_gap', 'volume_ratio', 'returns', 'returns_3d', 'returns_5d',
    'volatility', 'momentum', 'atr_ratio',
]
MODEL_BUNDLE_VERSION = 1


class SimpleFallbackClassifier:
    """Small dependency-free classifier used when sklearn is unavailable."""

    def __init__(self) -> None:
        self.feature_columns: list[str] = []
        self.positive_center: dict[str, float] = {}
        self.negative_center: dict[str, float] = {}
        self.class_prior_positive: float = 0.5

    def fit(self, X: pd.DataFrame, y: pd.Series) -> "SimpleFallbackClassifier":
        frame = pd.DataFrame(X).copy()
        labels = pd.Series(y).astype(int).reset_index(drop=True)
        frame = frame.reset_index(drop=True)
        self.feature_columns = frame.columns.tolist()
        positives = frame[labels == 1]
        negatives = frame[labels == 0]
        if positives.empty or negatives.empty:
            raise ValueError("training labels must contain both positive and negative samples")
        self.positive_center = {column: float(positives[column].mean()) for column in self.feature_columns}
        self.negative_center = {column: float(negatives[column].mean()) for column in self.feature_columns}
        self.class_prior_positive = float(labels.mean())
        return self

    def _score_row(self, row: pd.Series) -> float:
        positive_distance = 0.0
        negative_distance = 0.0
        for column in self.feature_columns:
            value = float(row[column])
            positive_distance += abs(value - self.positive_center[column])
            negative_distance += abs(value - self.negative_center[column])
        margin = negative_distance - positive_distance
        prior_bias = (self.class_prior_positive - 0.5) * 2.0
        scaled = (margin / max(len(self.feature_columns), 1)) + prior_bias
        return 1.0 / (1.0 + math.exp(-scaled))

    def predict_proba(self, X: pd.DataFrame):
        frame = pd.DataFrame(X).reindex(columns=self.feature_columns, fill_value=0.0)
        probabilities: list[list[float]] = []
        for _, row in frame.iterrows():
            up_probability = self._score_row(row)
            probabilities.append([1.0 - up_probability, up_probability])
        return probabilities

    def predict(self, X: pd.DataFrame):
        return [1 if probability[1] >= 0.5 else 0 for probability in self.predict_proba(X)]


@dataclass(frozen=True)
class MLSignalDecision:
    used_model: bool = False
    prediction: int = 0
    probability_up: float = 0.5
    aligned_confidence: float = 0.5
    gate_passed: bool = True
    reason: str = "ml_unavailable"


class MarketAdaptiveMLEngine:
    def __init__(self, *, enabled: bool = True, model_path: str = "data/ml_models") -> None:
        self.enabled = bool(enabled)
        self.model_path = Path(model_path)
        self.model_path.mkdir(parents=True, exist_ok=True)

    def _symbol_to_name(self, symbol: str) -> str:
        return str(symbol or "BTC_USDT").replace('/', '_').replace(':', '_')

    def _load_model_bundle(self, symbol: str) -> dict[str, Any] | None:
        path = self.model_path / f"{self._symbol_to_name(symbol)}_model.pkl"
        if not path.exists():
            return None
        try:
            with path.open('rb') as handle:
                payload = pickle.load(handle)
        except Exception:
            return None

        if isinstance(payload, dict) and 'model' in payload:
            return payload
        return {
            'bundle_version': 0,
            'model': payload,
            'feature_columns': list(FEATURE_COLUMNS),
        }

    def _load_model(self, symbol: str) -> Any | None:
        bundle = self._load_model_bundle(symbol)
        if bundle is None:
            return None
        return bundle.get('model')

    def _calc_feature_frame(self, df: pd.DataFrame) -> pd.DataFrame:
        features = pd.DataFrame(index=df.index)
        features['close'] = df['close']
        features['volume'] = df['volume']

        delta = df['close'].diff()
        gain = delta.where(delta > 0, 0.0)
        loss = -delta.where(delta < 0, 0.0)
        avg_gain = gain.rolling(14).mean()
        avg_loss = loss.rolling(14).mean()
        rs = avg_gain / (avg_loss + 1e-10)
        features['RSI'] = 100 - (100 / (1 + rs))

        ema12 = df['close'].ewm(span=12).mean()
        ema26 = df['close'].ewm(span=26).mean()
        features['MACD'] = ema12 - ema26
        features['MACD_signal'] = features['MACD'].ewm(span=9).mean()
        features['MACD_hist'] = features['MACD'] - features['MACD_signal']

        bb_mid = df['close'].rolling(20).mean()
        bb_std = df['close'].rolling(20).std()
        features['BB_upper'] = bb_mid + 2 * bb_std
        features['BB_lower'] = bb_mid - 2 * bb_std
        features['BB_position'] = (df['close'] - features['BB_lower']) / (features['BB_upper'] - features['BB_lower'] + 1e-10)

        features['MA5'] = df['close'].rolling(5).mean()
        features['MA20'] = df['close'].rolling(20).mean()
        features['MA60'] = df['close'].rolling(60).mean()
        features['MA5_ratio'] = features['MA5'] / (features['MA20'] + 1e-10)
        features['trend_gap'] = (features['MA20'] - features['MA60']) / (features['MA60'] + 1e-10)

        features['volume_ma20'] = df['volume'].rolling(20).mean()
        features['volume_ratio'] = df['volume'] / (features['volume_ma20'] + 1e-10)

        features['returns'] = df['close'].pct_change()
        features['returns_3d'] = df['close'].pct_change(3)
        features['returns_5d'] = df['close'].pct_change(5)
        features['volatility'] = features['returns'].rolling(20).std()
        features['momentum'] = df['close'] / df['close'].shift(10) - 1

        prev_close = df['close'].shift(1)
        tr = pd.concat([
            (df['high'] - df['low']),
            (df['high'] - prev_close).abs(),
            (df['low'] - prev_close).abs(),
        ], axis=1).max(axis=1)
        features['ATR'] = tr.rolling(14).mean()
        features['atr_ratio'] = features['ATR'] / (df['close'] + 1e-10)
        return features

    def _prepare_features(self, execution_frame: pd.DataFrame, *, feature_columns: list[str] | None = None) -> pd.DataFrame | None:
        if execution_frame is None or len(execution_frame) < 80:
            return None
        frame = execution_frame[["open", "high", "low", "close", "volume"]].copy()
        features = self._calc_feature_frame(frame)
        features = features.tail(1).fillna(0.0)
        columns = list(feature_columns or FEATURE_COLUMNS)
        return features.reindex(columns=columns, fill_value=0.0)

    def evaluate(self, *, symbol: str, execution_frame: pd.DataFrame, direction: int, min_confidence: float = 0.60) -> MLSignalDecision:
        if not self.enabled:
            return MLSignalDecision(used_model=False, gate_passed=True, reason="ml_disabled")
        if direction == 0:
            return MLSignalDecision(used_model=False, gate_passed=True, reason="no_direction")
        bundle = self._load_model_bundle(symbol)
        if bundle is None:
            return MLSignalDecision(used_model=False, gate_passed=True, reason="ml_model_missing")
        model = bundle.get('model')
        features = self._prepare_features(execution_frame, feature_columns=list(bundle.get('feature_columns') or FEATURE_COLUMNS))
        if features is None:
            return MLSignalDecision(used_model=False, gate_passed=True, reason="ml_insufficient_features")
        try:
            prediction = int(model.predict(features)[0])
            if hasattr(model, 'predict_proba'):
                probability_up = float(model.predict_proba(features)[0][1])
            else:
                probability_up = 1.0 if prediction == 1 else 0.0
        except Exception:
            return MLSignalDecision(used_model=False, gate_passed=True, reason="ml_predict_failed")

        aligned_confidence = probability_up if direction > 0 else (1.0 - probability_up)
        gate_passed = aligned_confidence >= float(min_confidence)
        reason = "ml_aligned" if gate_passed else "ml_low_confidence_or_counter_direction"
        return MLSignalDecision(
            used_model=True,
            prediction=prediction,
            probability_up=probability_up,
            aligned_confidence=aligned_confidence,
            gate_passed=gate_passed,
            reason=reason,
        )


class MarketAdaptiveModelTrainer:
    def __init__(self, *, model_path: str = "data/ml_models") -> None:
        self.model_path = Path(model_path)
        self.model_path.mkdir(parents=True, exist_ok=True)

    def _symbol_to_name(self, symbol: str) -> str:
        return str(symbol or "BTC_USDT").replace('/', '_').replace(':', '_')

    def _validate_training_frame(self, historical_data: pd.DataFrame) -> pd.DataFrame:
        required = ['open', 'high', 'low', 'close', 'volume']
        missing = [column for column in required if column not in historical_data.columns]
        if missing:
            raise ValueError(f"historical_data missing columns: {', '.join(missing)}")
        frame = historical_data[required].copy().reset_index(drop=True)
        frame = frame.apply(pd.to_numeric, errors='coerce')
        frame = frame.dropna().reset_index(drop=True)
        if frame.empty:
            raise ValueError('historical_data is empty after numeric cleanup')
        return frame

    def _build_classifier(self):
        try:
            from sklearn.ensemble import RandomForestClassifier
        except ModuleNotFoundError:
            return SimpleFallbackClassifier(), "simple_fallback"
        return RandomForestClassifier(
            n_estimators=200,
            max_depth=8,
            min_samples_split=20,
            min_samples_leaf=8,
            class_weight='balanced_subsample',
            random_state=42,
            n_jobs=-1,
        ), "random_forest"

    @staticmethod
    def _compute_binary_metrics(y_true: pd.Series, y_pred: Any) -> dict[str, float]:
        truth = pd.Series(y_true).astype(int).reset_index(drop=True)
        pred = pd.Series(list(y_pred)).astype(int).reset_index(drop=True)
        if truth.empty:
            return {
                'accuracy': 0.0,
                'precision': 0.0,
                'recall': 0.0,
                'f1': 0.0,
            }
        tp = int(((pred == 1) & (truth == 1)).sum())
        tn = int(((pred == 0) & (truth == 0)).sum())
        fp = int(((pred == 1) & (truth == 0)).sum())
        fn = int(((pred == 0) & (truth == 1)).sum())
        accuracy = float((tp + tn) / len(truth))
        precision = float(tp / (tp + fp)) if (tp + fp) > 0 else 0.0
        recall = float(tp / (tp + fn)) if (tp + fn) > 0 else 0.0
        f1 = float((2 * precision * recall) / (precision + recall)) if (precision + recall) > 0 else 0.0
        return {
            'accuracy': accuracy,
            'precision': precision,
            'recall': recall,
            'f1': f1,
        }

    def train(self, *, symbol: str, historical_data: pd.DataFrame, label_horizon: int = 3, min_return_threshold: float = 0.002) -> dict[str, Any]:
        label_horizon = max(1, int(label_horizon))
        min_return_threshold = float(min_return_threshold)
        engine = MarketAdaptiveMLEngine(enabled=True, model_path=str(self.model_path))
        frame = self._validate_training_frame(historical_data)
        feats = engine._calc_feature_frame(frame)

        future_return = frame['close'].shift(-label_horizon) / frame['close'] - 1.0
        dataset = pd.concat([
            feats[FEATURE_COLUMNS],
            pd.DataFrame({
                'future_return': future_return,
                'target': (future_return > min_return_threshold).astype('float64'),
            }),
        ], axis=1)
        dataset = dataset.iloc[:-label_horizon].dropna().reset_index(drop=True)
        dataset['target'] = dataset['target'].astype(int)

        if len(dataset) < 120:
            raise ValueError(f'not enough rows to train: {len(dataset)}')
        label_counts = dataset['target'].value_counts().to_dict()
        if len(label_counts) < 2:
            raise ValueError('training labels must contain both positive and negative samples')

        split_idx = int(len(dataset) * 0.8)
        split_idx = min(max(split_idx, 1), len(dataset) - 1)
        X_train = dataset.loc[:split_idx - 1, FEATURE_COLUMNS]
        y_train = dataset.loc[:split_idx - 1, 'target']
        X_test = dataset.loc[split_idx:, FEATURE_COLUMNS]
        y_test = dataset.loc[split_idx:, 'target']

        model, model_type = self._build_classifier()
        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)
        score_metrics = self._compute_binary_metrics(y_test, y_pred)
        trained_at = datetime.now(timezone.utc).isoformat()
        metrics = {
            'symbol': symbol,
            'bundle_version': MODEL_BUNDLE_VERSION,
            'feature_columns': list(FEATURE_COLUMNS),
            'model_type': model_type,
            'dataset_samples': int(len(dataset)),
            'positive_labels': int(label_counts.get(1, 0)),
            'negative_labels': int(label_counts.get(0, 0)),
            'train_samples': int(len(X_train)),
            'test_samples': int(len(X_test)),
            'accuracy': float(score_metrics['accuracy']),
            'precision': float(score_metrics['precision']),
            'recall': float(score_metrics['recall']),
            'f1': float(score_metrics['f1']),
            'label_horizon': int(label_horizon),
            'min_return_threshold': min_return_threshold,
            'trained_at': trained_at,
        }
        bundle = {
            'bundle_version': MODEL_BUNDLE_VERSION,
            'trained_at': trained_at,
            'symbol': symbol,
            'feature_columns': list(FEATURE_COLUMNS),
            'model_type': model_type,
            'model': model,
            'metrics': metrics,
        }
        model_file = self.model_path / f"{self._symbol_to_name(symbol)}_model.pkl"
        metrics_file = self.model_path / f"{self._symbol_to_name(symbol)}_metrics.json"
        tmp_model_file = model_file.with_suffix('.pkl.tmp')
        tmp_metrics_file = metrics_file.with_suffix('.json.tmp')
        with tmp_model_file.open('wb') as handle:
            pickle.dump(bundle, handle)
        tmp_metrics_file.write_text(json.dumps(metrics, ensure_ascii=False, indent=2), encoding='utf-8')
        tmp_model_file.replace(model_file)
        tmp_metrics_file.replace(metrics_file)
        return metrics
