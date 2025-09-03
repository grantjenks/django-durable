from __future__ import annotations

import random
from typing import Mapping


def compute_backoff(policy: Mapping[str, float], attempt: int) -> float:
    """Compute the delay before the next retry attempt.

    Parameters
    ----------
    policy:
        Mapping of retry policy options.
    attempt:
        The attempt number that just failed (1-based).
    """
    strategy = policy.get('strategy', 'exponential')
    initial = float(policy.get('initial_interval', 1.0))
    if strategy == 'linear':
        interval = initial * attempt
    else:
        coeff = float(policy.get('backoff_coefficient', 2.0))
        interval = initial * (coeff ** max(0, attempt - 1))
    max_interval = policy.get('maximum_interval')
    if max_interval is not None:
        interval = min(interval, float(max_interval))
    jitter = float(policy.get('jitter', 0.0) or 0.0)
    if jitter:
        delta = interval * jitter
        interval += random.uniform(-delta, delta)
    return max(interval, 0.0)
