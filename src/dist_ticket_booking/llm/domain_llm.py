"""Domain LLM integration for vaccine booking recommendations."""
import logging
import torch
from transformers import pipeline

logger = logging.getLogger(__name__)

SYSTEM_CONTEXT = (
    "You are the helpdesk support bot for the Vaccine Booking System. "
    "Answer citizen questions about vaccine appointments based strictly on the following rules. "
    "Rules: "
    "1. COVISHIELD: minimum gap between Dose 1 and Dose 2 is 84 days (12 weeks). "
    "2. COVAXIN: minimum gap between Dose 1 and Dose 2 is 28 days (4 weeks). "
    "3. Booster (Precaution) dose is allowed only 9 months (39 weeks) after completing Dose 2. "
    "4. Age eligibility: 18+ for COVISHIELD and COVAXIN; 15-17 years for CORBEVAX only. "
    "5. Required documents for registration: Any government-issued photo ID "
    "(Aadhaar card, Voter ID, Passport, Driving License). "
    "6. Slot cancellation is allowed up to 24 hours before the scheduled appointment. "
    "7. Government center slots are free of charge. Private center slots may cost Rs. 150 to Rs. 250. "
    "8. Each citizen can book a maximum of 1 slot per appointment session. "
    "Do not provide medical or clinical advice. Only answer booking-related questions."
)


class DomainLLM:
    """Local LLM adapter for answering domain questions."""

    def __init__(self, model_name: str = "Qwen/Qwen2.5-0.5B-Instruct"):
        self.model_name = model_name
        self.pipeline = None

        try:
            logger.info(f"Loading model '{self.model_name}'... This may take a moment.")
            self.pipeline = pipeline(
                "text-generation",
                model=self.model_name,
                device="cpu",
                torch_dtype=torch.float32,
            )
            logger.info(f"Model '{self.model_name}' loaded successfully.")
        except Exception as e:
            logger.error(
                f"Failed to load model '{self.model_name}': {e}. "
                "The service will return a default error message for queries."
            )

    def generate_recommendation(self, prompt: str) -> dict:
        """Run the prompt through the LLM and return its response."""
        if self.pipeline is None:
            return {
                "model": self.model_name,
                "prompt": prompt,
                "response": (
                    "Service unavailable. "
                    "Please check server logs and ensure model weights are accessible."
                ),
                "status": "error",
            }

        messages = [
            {"role": "system", "content": SYSTEM_CONTEXT},
            {"role": "user", "content": prompt},
        ]

        try:
            outputs = self.pipeline(
                messages,
                max_new_tokens=200,
                do_sample=True,
                temperature=0.3,
            )
            generated_text = outputs[0]["generated_text"]
            reply = (
                generated_text[-1]["content"]
                if isinstance(generated_text, list)
                else generated_text
            )
            return {
                "model": self.model_name,
                "prompt": prompt,
                "response": reply.strip(),
                "status": "ready",
            }
        except Exception as e:
            logger.error(f"Model inference error: {e}")
            return {
                "model": self.model_name,
                "prompt": prompt,
                "response": f"Model inference error: {e}",
                "status": "error",
            }

    def get_llm_answer(self, request_id: str, query: str, context: str = "") -> dict:
        full_prompt = f"{query}\n\nContext: {context}".strip() if context else query
        res = self.generate_recommendation(full_prompt)
        return {
            "request_id": request_id,
            "answer": res.get("response", ""),
            "status": res.get("status", "error"),
        }
