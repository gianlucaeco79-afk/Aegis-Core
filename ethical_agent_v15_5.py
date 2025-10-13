"""
Ethical Agent V15.5 - Infrastructure Abstraction (Final Deployment Readiness)
"""

__author__ = "Gianluca E."

import os
import time
import asyncio
import logging
import json
import secrets as py_secrets 
import httpx 
from typing import List, Dict, Any, Optional, Tuple, Set, Callable, Awaitable
from collections import deque, defaultdict
from contextlib import suppress
from datetime import datetime, timezone
from abc import ABC, abstractmethod

# Librerie installate
import numpy as np
from fastapi import FastAPI, HTTPException, Depends, Header
from pydantic import BaseModel, Field
from prometheus_client import Gauge, Counter, generate_latest, CONTENT_TYPE_LATEST
from fastapi_limiter import FastAPILimiter
from fastapi_limiter.depends import RateLimiter 


# --- CONFIG MANAGER ---
class ConfigManager(BaseModel): 
    REDIS_NODES: List[str] = Field(default_factory=lambda: os.getenv("REDIS_NODES", "mock-node-1,mock-node-2").split(','))
    MAX_EVENT_RETRIES: int = Field(default=int(os.getenv("MAX_EVENT_RETRIES", "3")), ge=1, le=10)
try: config = ConfigManager()
except Exception as e: raise SystemExit(f"FATAL: Invalid Configuration: {e}") from e


# --- SECRETS MANAGER (MOCK) ---
class SecretsManager:
    def get_tenant_api_key(self, tenant_id: str) -> str:
        return f"key_{tenant_id}_secret"

secrets_manager = SecretsManager()


# --- TENANT MANAGER (MOCK) ---
class TenantManager:
    def get_tenant_plan(self, tenant_id: str) -> str:
        if "premium" in tenant_id.lower(): return "PREMIUM"
        if "standard" in tenant_id.lower(): return "STANDARD"
        return "FREE"

tenant_manager = TenantManager()


# --- OBSERVABILITY SERVICE ---
class MockTracer:
    def start_as_current_span(self, name): return self
    def __enter__(self): return self
    def __exit__(self, exc_type, exc_val, exc_tb): pass
    def set_attribute(self, key, value): pass

class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_record = {"timestamp": datetime.fromtimestamp(record.created, timezone.utc).isoformat(), "level": record.levelname, "message": record.getMessage(), "extra": getattr(record, 'extra', {})} 
        def default_serializer(o):
            if isinstance(o, np.floating): return float(o)
            if isinstance(o, np.integer): return int(o)
            raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")
        return json.dumps(log_record, default=default_serializer)

class ObservabilityService:
    def __init__(self):
        self.tracer = MockTracer()
        self.logger = logging.getLogger("ethical_agent_v15_5")
        if not self.logger.handlers:
             handler = logging.StreamHandler()
             handler.setFormatter(JsonFormatter())
             self.logger.handlers = [handler]
        self.logger.setLevel(logging.INFO)
    
    def get_logger(self): return self.logger
    def get_tracer(self): return self.tracer

observability_service = ObservabilityService()
logger = observability_service.get_logger()
tracer = observability_service.get_tracer()


# --- CONFIG SERVICE ---
class ConfigService:
    def __init__(self):
        self._config = {
            "ETHICAL_PLANS": {"PREMIUM": {"CRITICAL": 0.75, "HIGH": 0.55, "MEDIUM": 0.35}, "STANDARD": {"CRITICAL": 0.7, "HIGH": 0.5, "MEDIUM": 0.3}, "FREE": {"CRITICAL": 0.6, "HIGH": 0.4, "MEDIUM": 0.2}},
            "RATE_LIMITS": {"PREMIUM": "500/minute", "STANDARD": "100/minute", "FREE": "20/minute"}
        }
        self._lock = asyncio.Lock() 
        
    async def _reload_config(self):
        if self._lock.locked():
            logger.debug("ConfigService: Reload skipped, another instance holds the lock.")
            return

        async with self._lock: 
            await asyncio.sleep(0.001) 
            if np.random.rand() < 0.1: 
                logger.info("ConfigService: Simulating dynamic ethical threshold update for PREMIUM plan (Lock Acquired).")
                self._config["ETHICAL_PLANS"]["PREMIUM"] = {"CRITICAL": 0.8, "HIGH": 0.6, "MEDIUM": 0.4}
                self._config["RATE_LIMITS"]["PREMIUM"] = "600/minute"
        
    def get_ethical_thresholds_by_plan(self, plan: str) -> Dict[str, float]:
        return self._config["ETHICAL_PLANS"].get(plan, self._config["ETHICAL_PLANS"]["STANDARD"])
    
    def get_rate_limit_by_plan(self, plan: str) -> str:
        return self._config["RATE_LIMITS"].get(plan, self._config["RATE_LIMITS"]["FREE"])
    
config_service = ConfigService()


# --- MESSAGE QUEUE INTERFACE & MOCK ---
start_time = time.time() 

class MessageQueueInterface(ABC):
    @abstractmethod
    async def send_message(self, topic: str, message: Dict[str, Any]) -> None: pass

class MockMessageQueue(MessageQueueInterface):
    def __init__(self, max_retries: int):
        self._max_retries = max_retries
        self._events_sent = []

    async def _send_attempt(self, topic: str, message: Dict[str, Any]):
        logger.debug(f"Attempting to send to {topic}")
        if np.random.rand() < 0.05: 
             raise IOError("Simulated temporary network failure.")
        await asyncio.sleep(0.001) 
        self._events_sent.append({"topic": topic, "message": message})

    async def send_message(self, topic: str, message: Dict[str, Any]) -> None:
        for attempt in range(self._max_retries):
            try:
                await self._send_attempt(topic, message)
                logger.info(f"Message successfully queued to {topic} after {attempt+1} attempts.")
                return
            except (IOError, httpx.RequestError, Exception) as e:
                if attempt < self._max_retries - 1:
                    wait_time = 2 ** attempt
                    logger.warning(f"Queue send failed (Attempt {attempt+1}/{self._max_retries}). Retrying in {wait_time}s. Error: {type(e).__name__}")
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(f"Message permanently failed to queue to {topic} after {self._max_retries} attempts. FINAL FAILURE.", exc_info=False)
                    raise RuntimeError("Failed to queue critical event.") from e 

message_queue = MockMessageQueue(config.MAX_EVENT_RETRIES)


# --- EVENT PUBLISHER ---
class EventPublisher:
    async def publish_ethical_violation(self, event_data: Dict[str, Any]):
        logger.warning(f"CRITICAL EVENT DETECTED: Starting queuing with retry for {event_data.get('tenant_id')}", extra=event_data)
        
        event_topic = "ETHICAL_VIOLATIONS"
        
        async def queue_task():
            try:
                await message_queue.send_message(event_topic, event_data)
            except RuntimeError as e:
                logger.critical(f"UNRECOVERABLE FAILURE: Event delivery failed permanently. SRE PAGER NEEDED. {e}")

        asyncio.ensure_future(queue_task())

event_publisher = EventPublisher()


# --- MOTORE ETICO & AGENTE ---

class BiasDetector:
    def __init__(self, severity_thresholds: Dict[str, float]): self.bias_metrics_thresholds = {"magnitude": 1.0, "imbalance": 0.3, "outlier": 2.0}; self.severity_thresholds = severity_thresholds
    
    # FUNZIONE GINI CORRETTA
    def _calculate_gini(self, state: np.ndarray) -> float:
        """Calcola l'indice di Gini su un array numpy."""
        if state.sum() == 0 or len(state) == 0: 
            return 0.0
        
        sorted_state = np.sort(state)
        n = len(sorted_state)
        index = np.arange(1, n + 1)
        
        gini = (2 * np.sum(index * sorted_state)) / (n * np.sum(sorted_state)) - (n + 1) / n
        return float(np.clip(gini, 0.0, 1.0))
        
    def detect_bias(self, state: np.ndarray) -> Dict[str, Any]:
        mean_per_dim = np.mean(state); gini_score = self._calculate_gini(state); std_dev = np.std(state)
        bias_components = {"magnitude_risk": float(np.clip(mean_per_dim / self.bias_metrics_thresholds["magnitude"], 0.0, 1.0)), "imbalance_risk": float(np.clip(gini_score / self.bias_metrics_thresholds["imbalance"], 0.0, 1.0)), "outlier_risk": float(np.clip(std_dev / mean_per_dim * 0.5, 0.0, 1.0)) if mean_per_dim > 0 else 0.0,}
        composite_bias_score = (0.4 * bias_components["magnitude_risk"] + 0.4 * bias_components["imbalance_risk"] + 0.2 * bias_components["outlier_risk"]); composite_bias_score = float(np.clip(composite_bias_score, 0.0, 1.0))
        if composite_bias_score > self.severity_thresholds["CRITICAL"]: severity = "CRITICAL"
        elif composite_bias_score > self.severity_thresholds["HIGH"]: severity = "HIGH"
        elif composite_bias_score > self.severity_thresholds["MEDIUM"]: severity = "MEDIUM"
        else: severity = "LOW"
        return {"is_biased": severity in ["CRITICAL", "HIGH"], "bias_score": composite_bias_score, "severity": severity, "components": bias_components, "flagged_dimensions": []}

class EthicalDecisionEngine:
    def __init__(self): self.max_iaec_penalty = 0.5; self.min_iaec = 0.1
    async def analyze(self, state: np.ndarray, tenant_id: Optional[str]) -> Dict[str, Any]:
        plan = tenant_manager.get_tenant_plan(tenant_id or "global")
        thresholds = config_service.get_ethical_thresholds_by_plan(plan)
        bias_detector = BiasDetector(thresholds)  
        bias_analysis = bias_detector.detect_bias(state); bias_score = bias_analysis["bias_score"]; severity = bias_analysis["severity"]
        base_iaec = float(np.clip(1.0 - bias_score, self.min_iaec, 0.95))
        if severity == "CRITICAL": action = "intervene_or_reject"; final_iaec = base_iaec * self.max_iaec_penalty
        elif severity == "HIGH": action = "intervene_or_reject"; final_iaec = base_iaec * 0.75
        elif severity == "MEDIUM": action = "warn"; final_iaec = base_iaec * 0.9
        else: action = "cooperate"; final_iaec = base_iaec
        final_iaec = float(np.clip(final_iaec, self.min_iaec, 1.0))
        return {"iaec": final_iaec, "action": action, "ethical_analysis": {"bias_analysis": bias_analysis, "justification": self._generate_justification(bias_analysis, action)}}
    def _generate_justification(self, bias_analysis: Dict, action: str) -> str:
        if action == "intervene_or_reject": return (f"CRITICAL VIOLATION: Bias Severity {bias_analysis['severity']} (Score: {bias_analysis['bias_score']:.2f}). The decision is blocked.")
        elif action == "warn": return (f"WARNING: Bias Severity {bias_analysis['severity']} (Score: {bias_analysis['bias_score']:.2f}). Risk approaching critical threshold.")
        else: return "State within acceptable ethical boundaries. No intervention required."

ethical_engine = EthicalDecisionEngine()

class EthicalAgent:
    async def act(self, state: np.ndarray, tenant_id: Optional[str] = None) -> Dict[str,Any]:
        with tracer.start_as_current_span("agent_act_core"):
            analysis = await ethical_engine.analyze(state, tenant_id)
            if analysis['ethical_analysis']['bias_analysis']['severity'] in ["CRITICAL", "HIGH"]:
                event_data = {"tenant_id": tenant_id, "event_type": "ETHICAL_VIOLATION", "severity": analysis['ethical_analysis']['bias_analysis']['severity'], "bias_score": analysis['ethical_analysis']['bias_analysis']['bias_score'], "action_taken": analysis['action']}
                asyncio.ensure_future(event_publisher.publish_ethical_violation(event_data)) 
            return {"action": analysis['action'], "iaec": analysis['iaec'], "tenant_id": tenant_id, "ethical_analysis": analysis['ethical_analysis']}
agent = EthicalAgent()
