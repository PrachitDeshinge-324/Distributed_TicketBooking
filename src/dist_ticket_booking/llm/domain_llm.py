"""Domain LLM integration for vaccine booking FAQ support.

Runs entirely offline using a locally cached HuggingFace model.
No internet connection is required after the first model download.
Model weights are stored in the HuggingFace cache directory
(~/.cache/huggingface/hub/ by default).

Default model: Qwen/Qwen3.5-0.8B (hybrid linear/full attention).
The device is picked automatically: CUDA > Apple MPS > CPU.
On Apple Silicon (MPS, fp16) an answer takes ~2-4 s. On plain CPU the
Qwen3.5 linear-attention layers run a slow reference implementation
(~1-2 s per token), so the app server applies a deadline and falls back
to keyword answers if the model is too slow.

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
import re
import time
from typing import Optional

DEFAULT_MODEL = "Qwen/Qwen3.5-0.8B"
_MAX_NEW_TOKENS = 80
_TOKENS_PER_LIVE_LINE = 30   # extra budget per slot listed in the live context
_MAX_NEW_TOKENS_CAP = 320

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
    "6. Cancellation deadline: an appointment can be cancelled only up to 24 hours before "
    "its scheduled time. To cancel, use the cancel option with your booking ID; any fee paid is refunded. "
    "7. Government center slots are free. Private center slots may cost Rs. 150 to Rs. 250. "
    "8. Each citizen can book a maximum of 1 slot per appointment session. "
    "9. Booking: log in, check availability, then book a slot by its slot ID. "
    "Do not provide medical or clinical advice. Only answer booking-related questions. "
    "When live system data is provided, use it for questions about availability: "
    "mention every slot that is relevant, and state the price exactly as given "
    "(say 'free' only when the data says 'free of charge'). "
    "Never speculate about reasons, demand or future dates that are not in the data. "
    "If a 'Relevant policy' is given, restate it accurately and keep every number and "
    "time limit exactly as written. Use only the facts provided; never invent contact "
    "details, penalties or extra steps. Reply in at most 2 short sentences."
)

_THINK_RE = re.compile(r"<think>.*?</think>", re.DOTALL)
_SENTENCE_END_RE = re.compile(r"[.!?)](?=\s|$)")


def _trim_incomplete(text: str) -> str:
    """Drop a trailing half-sentence left when generation hit the token limit."""
    text = text.strip()
    if not text or text[-1] in ".!?)":
        return text
    ends = [m.end() for m in _SENTENCE_END_RE.finditer(text)]
    return text[: ends[-1]].strip() if ends else text


def _pick_device():
    import torch
    if torch.cuda.is_available():
        return "cuda", torch.float16
    if getattr(torch.backends, "mps", None) and torch.backends.mps.is_available():
        return "mps", torch.float16
    return "cpu", torch.float32


def _split_prompt(prompt: str):
    query, _, context = prompt.partition("\n\nContext:")
    return query.strip(), context.strip()

# Keyword fallback — used when the model is not available
_FAQ_FALLBACK = {
    "cancel": (
        "Cancellation is allowed only up to 24 hours before your scheduled appointment. "
        "To cancel, use the cancel option with your booking ID; any fee paid is refunded."
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
        "The booster (precaution) dose is allowed only once at least 9 months have "
        "passed since completing Dose 2."
    ),
    "age": (
        "COVISHIELD and COVAXIN are for ages 18+. "
        "CORBEVAX is for citizens aged 15 to 17 years."
    ),
}


_LIVE_MARKER = "Current live slot availability:"
_LIVE_QUERY_WORDS = ("available", "availability", "seat", "slot", "how many", "remaining", "left", "vacant", "open")


def _relevant_policy(query: str) -> str:
    """Policy snippets matching the query; grounds the small model's answer."""
    q = query.lower()
    return " ".join(ans for kw, ans in _FAQ_FALLBACK.items() if kw in q and kw != "available")


def _grounded(answer: str, query: str) -> str:
    """Guardrail: a small model sometimes drops or inverts a key policy number
    (e.g. the 24-hour cancellation deadline). If the matched policy contains a
    number the answer leaves out, or says 'within 24 hours' for a deadline that is
    'up to 24 hours before', return the verified policy text instead."""
    policy = _relevant_policy(query)
    if not policy:
        return answer
    missing = [n for n in re.findall(r"\d+", policy) if n not in re.findall(r"\d+", answer)]
    inverted = "cancel" in query.lower() and re.search(r"within\s+24\s+hours", answer, re.I)
    if missing or inverted:
        return policy
    return answer


def _keyword_fallback(prompt: str) -> str:
    # The app server appends live inventory as "Context: ..." — answer
    # availability questions from it so the bot reflects real Raft state.
    query, _, context = prompt.partition("\n\nContext:")
    q = query.lower()
    if _LIVE_MARKER in context and any(w in q for w in _LIVE_QUERY_WORDS) and "cancel" not in q:
        return context.strip()
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
        model_name: str = DEFAULT_MODEL,
        batch_window_s: float = _BATCH_WINDOW_S,
        max_new_tokens: int = _MAX_NEW_TOKENS,
    ):
        self.model_name = model_name
        self.batch_window_s = batch_window_s
        self.max_new_tokens = max_new_tokens
        self._pipeline = None  # (tokenizer, model) once loaded
        self.device = "cpu"
        self._request_queue: queue.Queue = queue.Queue()

        self._load_model()
        self._start_worker()

    # Model loading — offline-first

    def _load_model(self):
        try:
            import warnings
            import torch
            from transformers import AutoTokenizer, AutoModelForCausalLM
            from transformers.utils import logging as hf_logging

            warnings.filterwarnings("ignore")
            hf_logging.set_verbosity_error()

            self.device, dtype = _pick_device()
            logger.info(f"Loading '{self.model_name}' on {self.device} (offline-first)...")
            try:
                tokenizer = AutoTokenizer.from_pretrained(self.model_name, local_files_only=True)
                model = AutoModelForCausalLM.from_pretrained(
                    self.model_name, local_files_only=True, dtype=dtype
                )
            except Exception:
                logger.info(f"Local cache miss for '{self.model_name}'. Downloading (first run only)...")
                tokenizer = AutoTokenizer.from_pretrained(self.model_name)
                model = AutoModelForCausalLM.from_pretrained(self.model_name, dtype=dtype)

            if tokenizer.pad_token_id is None:
                tokenizer.pad_token_id = tokenizer.eos_token_id
            tokenizer.padding_side = "left"  # required for batched decoder-only generation

            model = model.to(self.device).eval()
            self._pipeline = (tokenizer, model)
            logger.info(f"Model '{self.model_name}' loaded successfully on {self.device}.")

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

    def _build_chat_text(self, tokenizer, prompt: str) -> str:
        query, context = _split_prompt(prompt)
        parts = []
        if context and context != "TicketClientFAQ":
            parts.append(f"Live system data:\n{context}")
        policy = _relevant_policy(query)
        if policy:
            parts.append(f"Relevant policy: {policy}")
        user = "\n\n".join(parts + [f"Question: {query}"]) if parts else query
        messages = [
            {"role": "system", "content": SYSTEM_CONTEXT},
            {"role": "user", "content": user},
        ]
        try:
            # Qwen3.x: disable the <think> reasoning block for fast, direct answers
            return tokenizer.apply_chat_template(
                messages, tokenize=False, add_generation_prompt=True, enable_thinking=False
            )
        except TypeError:
            return tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)

    def _run_pipeline_batch(self, prompts: list) -> list:
        """Run one batched (left-padded) greedy generation for all prompts."""
        if self._pipeline is None:
            return [_keyword_fallback(p) for p in prompts]

        import torch
        tokenizer, model = self._pipeline
        texts = [self._build_chat_text(tokenizer, p) for p in prompts]
        # Listing live slots needs more room than a one-line FAQ answer.
        live_lines = max(_split_prompt(p)[1].count("\n- ") for p in prompts)
        budget = min(self.max_new_tokens + _TOKENS_PER_LIVE_LINE * live_lines, _MAX_NEW_TOKENS_CAP)
        enc = tokenizer(texts, return_tensors="pt", padding=True).to(self.device)
        with torch.no_grad():
            out = model.generate(
                **enc,
                max_new_tokens=budget,
                do_sample=False,
                repetition_penalty=1.1,
                pad_token_id=tokenizer.pad_token_id,
            )
        gen = out[:, enc["input_ids"].shape[1]:]
        results = []
        for prompt, ids in zip(prompts, gen):
            text = _trim_incomplete(_THINK_RE.sub("", tokenizer.decode(ids, skip_special_tokens=True)))
            if not text:
                results.append(_keyword_fallback(prompt))
            else:
                results.append(_grounded(text, _split_prompt(prompt)[0]))
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
