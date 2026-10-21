"""
lstm.py — next-event LSTM (DeepLog-style), trained with PyTorch.

It reads a request's events in order and, at every step, predicts the next
event, like phone autocomplete. Trained on normal sessions only. A session's
score is how surprised the model was at its most surprising step, so a skipped
or extra step stands out even when every line looks perfectly normal.
"""

import warnings

import numpy as np
import torch
from torch import nn

from logad.models.dataset import Session
from logad.models.sequences import PAD, WINDOW, Vocab, log_softmax, surprise_per_session, windows

SEED = 42


class NextEventLSTM(nn.Module):
    """Last WINDOW events -> a score (logit) for every possible next event."""

    def __init__(self, vocab_size: int, embed_dim: int = 32, hidden_dim: int = 64):
        super().__init__()
        self.embedding = nn.Embedding(vocab_size, embed_dim, padding_idx=PAD)
        self.lstm = nn.LSTM(embed_dim, hidden_dim, batch_first=True)
        self.out = nn.Linear(hidden_dim, vocab_size)

    def forward(self, windows: torch.Tensor) -> torch.Tensor:  # [B, W] -> [B, V]
        hidden, _ = self.lstm(self.embedding(windows))
        return self.out(hidden[:, -1])


class LSTMDetector:
    name = "LSTM (event order)"

    def __init__(self, epochs: int = 6, batch_size: int = 512, lr: float = 2e-3):
        self.epochs = epochs
        self.batch_size = batch_size
        self.lr = lr

    def fit(self, sessions: list[Session], log=print):
        torch.manual_seed(SEED)
        self.vocab = Vocab([t for s in sessions for t in s.templates])
        self.model = NextEventLSTM(len(self.vocab))
        x, y, _ = windows(self.vocab, sessions)
        x, y = torch.from_numpy(x), torch.from_numpy(y)
        optimizer = torch.optim.Adam(self.model.parameters(), lr=self.lr)
        loss_fn = nn.CrossEntropyLoss()
        generator = torch.Generator().manual_seed(SEED)

        self.model.train()
        for epoch in range(1, self.epochs + 1):
            order = torch.randperm(len(y), generator=generator)
            total = 0.0
            for start in range(0, len(order), self.batch_size):
                batch = order[start : start + self.batch_size]
                optimizer.zero_grad()
                loss = loss_fn(self.model(x[batch]), y[batch])
                loss.backward()
                optimizer.step()
                total += loss.item() * len(batch)
            log(f"    epoch {epoch}/{self.epochs}  loss {total / len(y):.4f}")
        self.model.eval()
        return self

    def export_onnx(self, path) -> None:
        """Save the network for ONNX Runtime (live scoring needs no PyTorch)."""
        example = torch.zeros((1, WINDOW), dtype=torch.int64)
        with warnings.catch_warnings():
            # PyTorch warns about LSTM batch sizes and its older exporter; the
            # serving tests check ONNX matches PyTorch on whole batches.
            warnings.simplefilter("ignore")
            torch.onnx.export(
                self.model,
                (example,),
                str(path),
                input_names=["windows"],
                output_names=["logits"],
                dynamic_axes={"windows": {0: "batch"}, "logits": {0: "batch"}},
                dynamo=False,
            )

    @torch.no_grad()
    def logits(self, x: np.ndarray) -> np.ndarray:
        chunks = [x[i : i + 8192] for i in range(0, len(x), 8192)]
        out = [self.model(torch.from_numpy(chunk)).numpy() for chunk in chunks]
        return np.concatenate(out) if out else np.zeros((0, len(self.vocab)), np.float32)

    def score(self, sessions: list[Session]) -> np.ndarray:
        x, y, owners = windows(self.vocab, sessions)
        scores, _ = surprise_per_session(log_softmax(self.logits(x)), y, owners, len(sessions))
        return scores

    def explain(self, session: Session) -> str:
        """The most surprising step: what came before, what was expected, what came."""
        x, y, owners = windows(self.vocab, [session])
        log_probs = log_softmax(self.logits(x))
        _, [step] = surprise_per_session(log_probs, y, owners, 1)
        before = self.vocab.templates[x[step][-1]]
        expected = self.vocab.templates[int(np.argmax(log_probs[step]))]
        got = self.vocab.templates[y[step]]
        p = float(np.exp(log_probs[step][y[step]]))
        return f"after '{before}' expected '{expected}', got '{got}' (p={p:.4f})"
