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

Domain grounding (no fine-tuning needed): every FAQ prompt carries the
booking-policy passages retrieved for the question (llm/knowledge.py) and
the live, Raft-replicated slot data; the answer is then checked against
them. Identical questions over identical live data are answered from a
small cache, so repeated FAQs cost no inference at all.

Concurrency design:
  - A single background worker thread owns the pipeline exclusively,
    so concurrent gRPC callers never touch the model simultaneously.
  - The worker collects requests that arrive within a short batching
    window (default 60 ms) and runs them as a single batched forward
    pass — this is significantly faster than N sequential calls when
    multiple clients send queries at the same time.
"""
import json
import logging
import queue
import threading
import re
import time
from collections import OrderedDict

from dist_ticket_booking.llm.knowledge import KNOWLEDGE_BASE, best_passage, numbers, ranked, retrieve

SMALL_MODEL = "Qwen/Qwen3.5-0.8B"
LARGE_MODEL = "Qwen/Qwen3.5-2B"
# "auto": the 2B model where there is a GPU / Apple Silicon (fast enough, and it
# can read a whole page of policy plus live state), the 0.8B model on plain CPU.
# Override with --model or the LLM_MODEL environment variable.
DEFAULT_MODEL = "auto"
# The app server appends the full live state (all slots, the asking citizen's
# appointments, today's date) after this marker in the FAQ context.
STATE_MARKER = "[STATE]"
_MAX_NEW_TOKENS = 80
_TOKENS_PER_LIVE_LINE = 30   # extra budget per slot listed in the live context
_MAX_NEW_TOKENS_CAP = 320
_ANSWER_CACHE_SIZE = 256     # FAQ answers kept for identical (question, live data)

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
    "8. Each citizen can hold at most one active appointment per dose (Dose 1, Dose 2 or booster) at any centre, "
    "and each booking is for one person. A session cannot be booked once it has started. "
    "9. Booking: log in, check availability, then book a slot by its slot ID. "
    "You may also explain what each vaccine offered here is (maker, type, doses, age group) "
    "and how they differ, using only the 'Relevant policy' facts. "
    "Do not provide medical or clinical advice: for which vaccine suits someone, say to ask a doctor. "
    "Only answer questions about vaccine appointments and the vaccines offered. "
    "When live system data is provided, use it for questions about availability: "
    "mention every slot that is relevant, and state the price exactly as given "
    "(say 'free' only when the data says 'free of charge'). "
    "Never speculate about reasons, demand or future dates that are not in the data. "
    "If a 'Relevant policy' is given, restate it accurately and keep every number and "
    "time limit exactly as written. Use only the facts provided; never invent contact "
    "details, penalties or extra steps. Reply in at most 2 short sentences."
)

INTENT_SYSTEM = (
    "You convert a citizen's message to a vaccination booking system into JSON. "
    "Reply with ONE JSON object and nothing else, with keys: "
    '"action" ("book", "cancel" or "question"), "vaccine" (one of the allowed vaccines or null), '
    '"city" (one of the allowed cities or null), "dose" (1, 2 or 3 for a booster, or null), '
    '"booking_id" (an id like APPT-... if the message contains one, else null). '
    "Use null for anything the message does not say. Never guess."
)
INTENT_EXAMPLES = [
    ("I need my second covaxin shot in Bombay",
     {"action": "book", "vaccine": "COVAXIN", "city": "Mumbai", "dose": 2, "booking_id": None}),
    ("please cancel APPT-SLOT-CVS-D1-alice-12",
     {"action": "cancel", "vaccine": None, "city": None, "dose": None, "booking_id": "APPT-SLOT-CVS-D1-alice-12"}),
    ("what documents should I carry?",
     {"action": "question", "vaccine": None, "city": None, "dose": None, "booking_id": None}),
]

_THINK_RE = re.compile(r"<think>.*?</think>", re.DOTALL)
# Websites, e-mail addresses and phone numbers: the model has none to give, so any it writes is invented.
_CONTACT_RE = re.compile(r"(https?://|www\.|\b[\w.-]+@[\w-]+\.\w+|\b[\w-]+\.(?:com|in|org|gov|net)\b|\+\d{1,3}[ -]?\d[\d -]{6,}\d|\b\d{10,}\b|\b1800[ -]?\d{3}[ -]?\d{3,4}\b)",
                         re.I)
_NO_ANSWER = ("I can help with booking, cancelling and questions about vaccination appointments, such as "
              "dose gaps, documents, prices or cancellation. Try 'book a COVAXIN first dose in Mumbai'.")
_SENTENCE_END_RE = re.compile(r"[.!?)](?=\s|$)")


def _trim_incomplete(text: str) -> str:
    """Drop a trailing half-sentence left when generation hit the token limit."""
    text = text.strip()
    if not text or text[-1] in ".!?)":
        return text
    ends = [m.end() for m in _SENTENCE_END_RE.finditer(text)]
    return text[: ends[-1]].strip() if ends else text


def pick_model(name: str = DEFAULT_MODEL) -> str:
    import os
    name = os.environ.get("LLM_MODEL") or name
    if name != "auto":
        return name
    try:
        device, _ = _pick_device()
    except Exception:
        return SMALL_MODEL
    return LARGE_MODEL if device != "cpu" else SMALL_MODEL


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


def _split_state(context: str):
    """(focused availability context, full live state)."""
    focused, _, state = context.partition(STATE_MARKER)
    return focused.strip(), state.strip()


def _without_state(prompt: str) -> str:
    """The prompt as the passage pipeline (small model) expects it: no full-state block."""
    query, context = _split_prompt(prompt)
    focused, _ = _split_state(context)
    return f"{query}\n\nContext: {focused}" if focused else query


# --------------------------------------------------------------- page mode
# The larger model gets ONE page: every policy passage plus the live state,
# and answers freely. A thin sentence-level filter then drops only sentences
# that break an objective rule (seen in testing): invented contact details or
# numbers, the cancellation deadline read backwards, or a medical claim.

POLICY_PAGE = "\n".join("- " + p.text for p in KNOWLEDGE_BASE if p.topic != "medical")

PAGE_SYSTEM = (
    "You are the help desk of a COVID-19 vaccination booking system in India. Answer ONLY the question "
    "asked, in at most 2 short sentences, using only facts from the reference page and the live data. "
    "Do not add warnings, advice or facts that were not asked for. Mention the citizen's own appointments "
    "only if the question is about them. If the page does not answer the question, say you don't know. "
    "Do not judge vaccines: never call a vaccine safe, unsafe, better or worse, and never explain why a "
    "dose is medically needed; say that is a question for a doctor. Never invent websites, phone numbers, "
    "dates or numbers."
)
DOCTOR_LINE = "Medical questions (why a dose is needed, safety, side effects) are best answered by a doctor."
_MEDICAL_CLAIM_RE = re.compile(
    r"\b(safe|safer|safest|unsafe|immunity|immune|protects?|protection|protective|effective|efficacy|"
    r"side[- ]effects?|antibod\w*|strains?|variants?|infections?)\b", re.I)
_REFUSAL_RE = re.compile(r"\b(doctor|health worker|medical (advice|opinion)|do not (provide|give)|can'?t|cannot|"
                         r"not able|don'?t know)\b", re.I)
_INVERTED_CANCEL_RE = re.compile(r"\bcancel\w*\b.*\bwithin\s+(the\s+)?24\s+hours\b", re.I)
_NEGATION_RE = re.compile(r"\b(no longer|not|cannot|can'?t|closed|isn'?t|unable)\b", re.I)
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+")


def _page_guard(answer: str, query: str, state: str) -> str:
    known = set(numbers(POLICY_PAGE + " " + state + " " + query))
    keep, medical = [], False
    for s in _SENTENCE_SPLIT_RE.split(answer.strip()):
        if not s:
            continue
        if _CONTACT_RE.search(s):
            continue
        if any(n not in known for n in numbers(s)):
            continue
        if _INVERTED_CANCEL_RE.search(s) and not _NEGATION_RE.search(s):
            continue
        if _MEDICAL_CLAIM_RE.search(s) and not _REFUSAL_RE.search(s):
            medical = True
            continue
        keep.append(s)
    if medical and not any("doctor" in s.lower() for s in keep):
        keep.append(DOCTOR_LINE)
    if not keep:
        return best_passage(query) or _NO_ANSWER
    return " ".join(keep)

_LIVE_MARKER = "Current live slot availability:"
_LIVE_QUERY_WORDS = ("available", "availability", "seat", "slot", "how many", "remaining", "left", "vacant", "open")


_STOP = {"about", "after", "again", "against", "because", "before", "being", "below", "between", "could",
         "every", "first", "their", "there", "these", "those", "under", "until", "which", "while", "would",
         "should", "where", "other", "please", "least", "only", "also", "must", "will", "with", "your", "from",
         "that", "this", "have", "they", "them", "what", "when", "into", "than", "then", "such", "each"}


def _stem(w: str) -> str:
    return w[:5]


def _novel_words(answer: str, sources: str) -> tuple:
    """(content words of the answer found nowhere in the sources, all content words)."""
    known = {_stem(w) for w in re.findall(r"[a-z]+", sources.lower())}
    words = [w for w in re.findall(r"[a-z]+", answer.lower()) if len(w) >= 5 and w not in _STOP]
    return [w for w in words if _stem(w) not in known], words


def _grounded(answer: str, query: str, context: str = "") -> str:
    """Guardrail: a small model sometimes drops, invents or inverts a key number.

    The verified passage text replaces the answer when
      - the answer states a number found in none of the passages, the question
        or the live data (an invented gap, age, price or deadline);
      - the top passage is a strict rule (e.g. the 24-hour cancellation
        deadline) and the answer leaves out one of its numbers; or
      - it says 'within 24 hours' for a deadline that is 'up to 24 hours before';
      - the top passage is marked exact (comparisons between vaccines).
    Background passages (what a vaccine is, comparisons) may be summarised
    freely, so a good explanation is not swapped for a canned one.
    """
    passages = retrieve(query, k=2)
    if _CONTACT_RE.search(answer):  # invented website / e-mail / phone number
        return passages[0].text if passages else _NO_ANSWER
    if not passages:
        return answer
    top = passages[0]
    if top.exact:
        if top.topic == "medical":
            # Add the booking rule the question touches ("why is a booster needed" -> booster rule).
            rule = next((p for _, p in ranked(query) if p.topic != "medical"), None)
            return f"{top.text} {rule.text}" if rule else top.text
        return top.text
    # Invented content: most of the answer's content words appear in none of the
    # passages, the question, the live data or the system rules.
    novel, words = _novel_words(answer, " ".join(p.text for p in passages) + " " + query + " "
                                + context + " " + SYSTEM_CONTEXT)
    if len(novel) >= 4 and len(novel) > 0.4 * len(words):
        return top.text
    known = set(numbers(" ".join(p.text for p in passages) + " " + query + " " + context))
    said = numbers(answer)
    invented = [n for n in said if n not in known]
    missing = top.strict and [n for n in numbers(top.text) if n not in said]
    inverted = "cancel" in query.lower() and re.search(r"within\s+24\s+hours", answer, re.I)
    if invented or missing or inverted:
        return top.text
    return answer


_LIVE_LINE_RE = re.compile(
    r"^- (?P<name>.+?): (?:(?P<count>\d+) open slot\(s\) on (?P<date>\S+); price: (?P<price>.+)|SOLD OUT on (?P<sold_date>\S+))$"
)
_NEGATIVE_RE = re.compile(r"\b(no more|no (?:available|open|remaining)|none (?:left|remain)|sold out|fully booked|unavailable)\b", re.I)


def _live_summary(rows) -> str:
    parts = []
    for r in rows:
        if r["count"] is not None:
            parts.append(f"{r['name']}: {r['count']} open slot(s) on {r['date']} ({r['price']})")
        else:
            parts.append(f"{r['name']}: sold out on {r['sold_date']}")
    return "Live availability - " + "; ".join(parts) + "."


def _grounded_availability(answer: str, context: str) -> str:
    """Guardrail for live-availability answers.

    For a focused question (at most 3 matching slots) the answer must state
    every live seat count and must not claim 'no more / sold out' while seats
    are open (or the reverse). Otherwise the reply is built directly from the
    Raft-replicated data, so the assistant never contradicts the system state.
    """
    rows = [m.groupdict() for m in (_LIVE_LINE_RE.match(l.strip()) for l in context.splitlines()) if m]
    if not rows or len(rows) > 3:
        return answer
    numbers = set(re.findall(r"\d+", answer))
    missing = [r["count"] for r in rows if r["count"] is not None and r["count"] not in numbers]
    any_open = any(r["count"] is not None for r in rows)
    negative = bool(_NEGATIVE_RE.search(answer))
    contradicts = (any_open and negative) or (not any_open and not negative)
    if missing or contradicts:
        return _live_summary(rows)
    return answer


def _keyword_fallback(prompt: str) -> str:
    # The app server appends live inventory as "Context: ..." — answer
    # availability questions from it so the bot reflects real Raft state.
    query, _, context = prompt.partition("\n\nContext:")
    q = query.lower()
    if _LIVE_MARKER in context and any(w in q for w in _LIVE_QUERY_WORDS) and "cancel" not in q:
        return context.strip()
    policy = best_passage(query)
    if policy:
        return policy
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
        self.requested_model = model_name
        self.model_name = pick_model(model_name)
        self.batch_window_s = batch_window_s
        self.max_new_tokens = max_new_tokens
        self._pipeline = None  # (tokenizer, model) once loaded
        self.device = "cpu"
        self._request_queue: queue.Queue = queue.Queue()
        self._cache: "OrderedDict[str, str]" = OrderedDict()
        self._cache_lock = threading.Lock()
        self.cache_hits = 0

        self._load_model()
        self._start_worker()

    # Model loading — offline-first

    def _load_model(self):
        candidates = [self.model_name]
        if self.model_name != SMALL_MODEL and pick_model(self.requested_model) == self.model_name \
                and self.requested_model == "auto":
            candidates.append(SMALL_MODEL)  # auto: the small model if the large one can't load
        for name in candidates:
            if self._load_one(name):
                self.model_name = name
                break
        # Page mode for any model larger than the 0.8B one (see POLICY_PAGE).
        self.page_mode = self._pipeline is not None and self.model_name != SMALL_MODEL
        if self.page_mode:
            self._warm_up()

    def _load_one(self, name: str) -> bool:
        try:
            import warnings
            import torch
            from transformers import AutoTokenizer, AutoModelForCausalLM
            from transformers.utils import logging as hf_logging

            warnings.filterwarnings("ignore")
            hf_logging.set_verbosity_error()

            self.device, dtype = _pick_device()
            logger.info(f"Loading '{name}' on {self.device} (offline-first)...")
            try:
                tokenizer = AutoTokenizer.from_pretrained(name, local_files_only=True)
                model = AutoModelForCausalLM.from_pretrained(name, local_files_only=True, dtype=dtype)
            except Exception:
                logger.info(f"Local cache miss for '{name}'. Downloading (first run only)...")
                tokenizer = AutoTokenizer.from_pretrained(name)
                model = AutoModelForCausalLM.from_pretrained(name, dtype=dtype)

            if tokenizer.pad_token_id is None:
                tokenizer.pad_token_id = tokenizer.eos_token_id
            tokenizer.padding_side = "left"  # required for batched decoder-only generation

            model = model.to(self.device).eval()
            self._pipeline = (tokenizer, model)
            logger.info(f"Model '{name}' loaded successfully on {self.device}.")
            return True
        except Exception as exc:
            logger.warning(f"Could not load model '{name}': {exc}.")
            self._pipeline = None
            return False

    def _warm_up(self):
        """One throw-away answer, so the first real question does not pay the
        one-time GPU kernel compilation (~20 s on Apple MPS)."""
        try:
            t0 = time.monotonic()
            self._run_page_batch(["How do I cancel?\n\nContext: " + STATE_MARKER + "\nToday: -"])
            logger.info(f"Warm-up answer done in {time.monotonic() - t0:.1f}s.")
        except Exception as exc:
            logger.warning(f"Warm-up failed: {exc}")

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

            logger.debug(f"Processing batch of {len(batch)} query(ies).")
            # FAQ answers and intent extraction use different prompts and
            # decoding settings, so each kind runs as its own batched pass.
            for kind, runner in (("faq", self._run_pipeline_batch), ("intent", self._run_intent_batch)):
                group = [item for item in batch if item[3] == kind]
                if not group:
                    continue
                try:
                    answers = runner([item[0] for item in group])
                    for (_, event, holder, _), answer in zip(group, answers):
                        holder["response"] = answer
                        holder["status"] = "ready"
                        event.set()
                except Exception as exc:
                    logger.error(f"Batch inference error: {exc}")
                    for (_, event, holder, _) in group:
                        holder["response"] = f"Inference error: {exc}"
                        holder["status"] = "error"
                        event.set()

    def _build_intent_text(self, tokenizer, prompt: str) -> str:
        text, allowed = _split_prompt(prompt)
        messages = [{"role": "system", "content": f"{INTENT_SYSTEM} Allowed values - {allowed}"}]
        for example, answer in INTENT_EXAMPLES:
            messages.append({"role": "user", "content": example})
            messages.append({"role": "assistant", "content": json.dumps(answer)})
        messages.append({"role": "user", "content": text})
        try:
            return tokenizer.apply_chat_template(
                messages, tokenize=False, add_generation_prompt=True, enable_thinking=False
            )
        except TypeError:
            return tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)

    def _run_intent_batch(self, prompts: list) -> list:
        """Batched JSON intent extraction (greedy decoding)."""
        if self._pipeline is None:
            return [_rule_intent_json(p) for p in prompts]
        import torch
        tokenizer, model = self._pipeline
        texts = [self._build_intent_text(tokenizer, p) for p in prompts]
        enc = tokenizer(texts, return_tensors="pt", padding=True).to(self.device)
        with torch.no_grad():
            out = model.generate(**enc, max_new_tokens=80, do_sample=False,
                                 pad_token_id=tokenizer.pad_token_id)
        gen = out[:, enc["input_ids"].shape[1]:]
        return [_THINK_RE.sub("", tokenizer.decode(ids, skip_special_tokens=True)).strip() for ids in gen]

    def _build_chat_text(self, tokenizer, prompt: str) -> str:
        query, context = _split_prompt(prompt)
        parts = []
        if context and context != "TicketClientFAQ":
            parts.append(f"Live system data:\n{context}")
        passages = retrieve(query, k=2)
        if passages:
            parts.append("Relevant policy: " + " ".join(p.text for p in passages))
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

    def _build_page_text(self, tokenizer, prompt: str) -> str:
        query, context = _split_prompt(prompt)
        _, state = _split_state(context)
        messages = [
            {"role": "system", "content": PAGE_SYSTEM},
            {"role": "user", "content": f"Reference page:\n{POLICY_PAGE}\n\nLive data:\n{state or '(none)'}"
                                        f"\n\nQuestion: {query}"},
        ]
        try:
            return tokenizer.apply_chat_template(
                messages, tokenize=False, add_generation_prompt=True, enable_thinking=False)
        except TypeError:
            return tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)

    def _run_page_batch(self, prompts: list) -> list:
        """Larger model: one page of policy + live state, a free answer, a thin filter."""
        import torch
        tokenizer, model = self._pipeline
        texts = [self._build_page_text(tokenizer, p) for p in prompts]
        enc = tokenizer(texts, return_tensors="pt", padding=True).to(self.device)
        with torch.no_grad():
            out = model.generate(**enc, max_new_tokens=90, do_sample=False, repetition_penalty=1.1,
                                 pad_token_id=tokenizer.pad_token_id)
        gen = out[:, enc["input_ids"].shape[1]:]
        results = []
        for prompt, ids in zip(prompts, gen):
            query, context = _split_prompt(prompt)
            text = _trim_incomplete(_THINK_RE.sub("", tokenizer.decode(ids, skip_special_tokens=True)))
            if not text:
                results.append(_keyword_fallback(_without_state(prompt)))
                continue
            focused, state = _split_state(context)
            answer = _page_guard(text, query, state)
            if _LIVE_MARKER in focused:  # same live-count guardrail as the small model
                answer = _grounded_availability(answer, focused)
            results.append(answer)
        return results

    def _run_pipeline_batch(self, prompts: list) -> list:
        """Run one batched (left-padded) greedy generation for all prompts."""
        if self._pipeline is not None and self.page_mode:
            return self._run_page_batch(prompts)
        prompts = [_without_state(p) for p in prompts]  # small model: retrieved passages only
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
                query, context = _split_prompt(prompt)
                text = _grounded(text, query, context)
                if _LIVE_MARKER in context:
                    text = _grounded_availability(text, context)
                results.append(text)
        return results

    # Public interface

    def generate_recommendation(self, prompt: str) -> dict:
        """Submit a prompt and block until the worker returns the answer."""
        return self._submit(prompt, "faq")

    def _submit(self, prompt: str, kind: str) -> dict:
        # The prompt includes the live slot data, so a cached answer is only
        # reused while that data is unchanged.
        key = f"{kind}\x00{prompt}"
        with self._cache_lock:
            cached = self._cache.get(key)
            if cached is not None:
                self._cache.move_to_end(key)
                self.cache_hits += 1
        if cached is not None:
            return {"model": self.model_name, "prompt": prompt, "response": cached,
                    "status": "ready", "cached": True}
        result = self._submit_uncached(prompt, kind)
        if result["status"] == "ready" and self._pipeline is not None and result["response"]:
            with self._cache_lock:
                self._cache[key] = result["response"]
                while len(self._cache) > _ANSWER_CACHE_SIZE:
                    self._cache.popitem(last=False)
        return result

    def _submit_uncached(self, prompt: str, kind: str) -> dict:
        result_holder: dict = {}
        done = threading.Event()
        self._request_queue.put((prompt, done, result_holder, kind))
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

    def extract_intent(self, request_id: str, text: str, allowed: str = "") -> dict:
        """JSON intent for a natural-language booking/cancellation request."""
        res = self._submit(f"{text}\n\nContext: {allowed}" if allowed else text, "intent")
        return {"request_id": request_id, "answer": res.get("response", ""), "status": res.get("status", "error")}


def _rule_intent_json(prompt: str) -> str:
    """Intent extraction without a model (keyword rules), as JSON text."""
    from dist_ticket_booking.llm.intent import rule_based_intent
    text, allowed = _split_prompt(prompt)
    m = re.search(r"cities:\s*([^;]*)", allowed)
    cities = [c.strip() for c in m.group(1).split(",") if c.strip()] if m else []
    return json.dumps(rule_based_intent(text, cities))
