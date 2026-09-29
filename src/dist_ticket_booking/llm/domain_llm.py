"""Domain LLM integration for vaccine booking FAQ support.

Runs entirely offline using a locally cached HuggingFace model.
No internet connection is required after the first model download.
Model weights are stored in the HuggingFace cache directory
(~/.cache/huggingface/hub/ by default).

Inference performance scales with the hardware running Node 1:
a modern CPU handles Qwen2.5-0.5B in a few seconds per query;
a GPU (CUDA or MPS) reduces this to under a second.

Concurrency design:
  - A single background worker thread owns the pipeline exclusively,
    so concurrent gRPC callers never touch the model simultaneously.
  - The worker collects requests that arrive within a short batching
    window (default 60 ms) and runs them as a single batched forward
    pass — this is significantly faster than N sequential calls when
    multiple clients send queries at the same time.
"""
import logging
import queue
import threading
import time
from typing import Optional

logger = logging.getLogger(__name__)

# Default batching window in seconds.
# The worker waits up to this long after the first queued request
# to collect more requests before running a batched inference call.
_BATCH_WINDOW_S = 0.06   # 60 ms

SYSTEM_CONTEXT = (
    "You are the helpdesk support bot for the Vaccine Booking System. "
    "Answer citizen questions about vaccine appointments based strictly on the following rules. "
    "Rules: "
    "1. COVISHIELD: minimum gap between Dose 1 and Dose 2 is 84 days (12 weeks). "
    "2. COVAXIN: minimum gap between Dose 1 and Dose 2 is 28 days (4 weeks). "
    "3. Booster (Precaution) dose is allowed only 9 months (39 weeks) after completing Dose 2. "
    "4. Age eligibility: 18+ for COVISHIELD and COVAXIN; 15-17 years for CORBEVAX only. "
    "5. Required documents: Any government-issued photo ID "
    "(Aadhaar card, Voter ID, Passport, Driving License). "
    "6. Slot cancellation is allowed up to 24 hours before the scheduled appointment. "
    "7. Government center slots are free. Private center slots may cost Rs. 150 to Rs. 250. "
    "8. Each citizen can book a maximum of 1 slot per appointment session. "
    "Do not provide medical or clinical advice. Only answer booking-related questions."
)

# Keyword fallback — used when the model is not available
_FAQ_FALLBACK = {
    "cancel": (
        "You can cancel your appointment up to 24 hours before the scheduled time "
        "using the cancellation option in your booking portal."
    ),
    "available": (
        "Slot availability depends on the vaccination center and date. "
        "Use the 'get availability' option to see current open slots."
    ),
    "document": (
        "You need any valid government-issued photo ID: Aadhaar card, Voter ID, "
        "Passport, or Driving License."
    ),
    "price": (
        "Government center slots are free of charge. "
        "Private center slots cost between Rs. 150 and Rs. 250."
    ),
    "covishield": (
        "The minimum gap between COVISHIELD Dose 1 and Dose 2 is 84 days (12 weeks)."
    ),
    "covaxin": (
        "The minimum gap between COVAXIN Dose 1 and Dose 2 is 28 days (4 weeks)."
    ),
    "booster": (
        "The booster (precaution) dose is allowed 9 months after completing Dose 2."
    ),
    "age": (
        "COVISHIELD and COVAXIN are for ages 18+. "
        "CORBEVAX is for citizens aged 15 to 17 years."
    ),
}


def _keyword_fallback(query: str) -> str:
    q = query.lower()
    for keyword, answer in _FAQ_FALLBACK.items():
        if keyword in q:
            return answer
    return (
        "For vaccine booking queries please contact the helpdesk or "
        "visit the official vaccination portal."
    )


class DomainLLM:
    """Offline HuggingFace LLM for domain FAQ queries.

    Loading strategy (fully offline after first run):
      1. Try local cache only (local_files_only=True) — works with no internet.
      2. If the model is not cached yet, attempt a one-time download.
      3. If both fail, fall back to keyword matching.

    Concurrency strategy (batched worker thread):
      - One background 'llm-worker' thread owns the pipeline.
      - On arrival of the first queued request the worker waits up to
        _BATCH_WINDOW_S for additional requests to accumulate, then
        runs all of them in a single batched pipeline call.
      - Batching amortizes the fixed per-call overhead across N queries,
        giving roughly N× throughput compared to N sequential calls.
    """

    def __init__(
        self,
        model_name: str = "Qwen/Qwen2.5-0.5B-Instruct",
        batch_window_s: float = _BATCH_WINDOW_S,
    ):
        self.model_name = model_name
        self.batch_window_s = batch_window_s
        self._pipeline = None
        self._request_queue: queue.Queue = queue.Queue()

        self._load_model()
        self._start_worker()

    # Model loading — offline-first

    def _load_model(self):
        try:
            import warnings
            import torch
            from transformers import AutoTokenizer, AutoModelForCausalLM, pipeline
            from transformers.utils import logging as hf_logging

            # Suppress all noisy deprecation/generation warnings from transformers
            warnings.filterwarnings("ignore")
            hf_logging.set_verbosity_error()

            logger.info(f"Loading '{self.model_name}' from local cache (offline mode)...")
            try:
                tokenizer = AutoTokenizer.from_pretrained(
                    self.model_name,
                    local_files_only=True,
                    clean_up_tokenization_spaces=False,
                )
                model = AutoModelForCausalLM.from_pretrained(
                    self.model_name,
                    local_files_only=True,
                    dtype=torch.float32,
                )
            except Exception:
                logger.info(
                    f"Local cache miss for '{self.model_name}'. "
                    "Downloading model (first run only)..."
                )
                tokenizer = AutoTokenizer.from_pretrained(
                    self.model_name,
                    local_files_only=False,
                    clean_up_tokenization_spaces=False,
                )
                model = AutoModelForCausalLM.from_pretrained(
                    self.model_name,
                    local_files_only=False,
                    dtype=torch.float32,
                )

            if tokenizer.pad_token_id is None:
                tokenizer.pad_token_id = tokenizer.eos_token_id

            self._pipeline = pipeline(
                "text-generation",
                model=model,
                tokenizer=tokenizer,
                device="cpu",
            )
            logger.info(f"Model '{self.model_name}' loaded successfully.")

        except Exception as exc:
            logger.warning(
                f"Could not load model '{self.model_name}': {exc}. "
                "All queries will use keyword fallback."
            )
            self._pipeline = None

    # Worker thread — batched inference

    def _start_worker(self):
        t = threading.Thread(
            target=self._inference_worker, daemon=True, name="llm-worker"
        )
        t.start()

    def _inference_worker(self):
        """Collect a batch of pending requests then run one forward pass."""
        while True:
            # Block until at least one request arrives
            first = self._request_queue.get()
            batch = [first]

            # Collect any additional requests that arrive within the window
            deadline = time.monotonic() + self.batch_window_s
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                try:
                    item = self._request_queue.get(timeout=remaining)
                    batch.append(item)
                except queue.Empty:
                    break

            prompts = [item[0] for item in batch]
            logger.debug(f"Processing batch of {len(batch)} query(ies).")

            try:
                answers = self._run_pipeline_batch(prompts)
                for (_, event, holder), answer in zip(batch, answers):
                    holder["response"] = answer
                    holder["status"] = "ready"
                    event.set()
            except Exception as exc:
                logger.error(f"Batch inference error: {exc}")
                for (_, event, holder) in batch:
                    holder["response"] = f"Inference error: {exc}"
                    holder["status"] = "error"
                    event.set()

    def _run_pipeline_batch(self, prompts: list) -> list:
        """Run the pipeline on a list of prompts in one batched call."""
        if self._pipeline is None:
            return [_keyword_fallback(p) for p in prompts]

        messages_batch = [
            [
                {"role": "system", "content": SYSTEM_CONTEXT},
                {"role": "user", "content": p},
            ]
            for p in prompts
        ]

        # batch_size tells the pipeline how many samples to process in one pass
        outputs = self._pipeline(
            messages_batch,
            max_new_tokens=200,
            do_sample=True,
            temperature=0.3,
            batch_size=len(messages_batch),
        )

        results = []
        for out in outputs:
            # pipeline returns a list-of-dicts per sample when given a batch
            generated = out[0]["generated_text"] if isinstance(out, list) else out["generated_text"]
            if isinstance(generated, list):
                results.append(generated[-1]["content"].strip())
            else:
                results.append(str(generated).strip())
        return results

    # Public interface

    def generate_recommendation(self, prompt: str) -> dict:
        """Submit a prompt and block until the worker returns the answer."""
        result_holder: dict = {}
        done = threading.Event()
        self._request_queue.put((prompt, done, result_holder))
        done.wait()
        return {
            "model": self.model_name,
            "prompt": prompt,
            "response": result_holder.get("response", ""),
            "status": result_holder.get("status", "error"),
        }

    def get_llm_answer(self, request_id: str, query: str, context: str = "") -> dict:
        full_prompt = f"{query}\n\nContext: {context}".strip() if context else query
        res = self.generate_recommendation(full_prompt)
        return {
            "request_id": request_id,
            "answer": res.get("response", ""),
            "status": res.get("status", "error"),
        }
