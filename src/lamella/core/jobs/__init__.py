# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Generic job runner with progress tracking.

Every handler in the app that (a) calls AI, (b) hits an external
API, or (c) iterates N items where N may be large should submit
the work as a job via :class:`~lamella.core.jobs.runner.JobRunner`
and return immediately with a ``job_id``. The browser displays a
progress modal that polls ``/jobs/{id}/partial`` until the job
reaches a terminal state.

See ``feedback_long_running_ops.md`` in Claude's memory and the
docstring on :class:`JobRunner` for the full contract.
"""
from lamella.core.jobs.context import JobCancelled, JobContext
from lamella.core.jobs.models import (
    Job,
    JobEvent,
    JobOutcome,
    JobStatus,
    TERMINAL_STATUSES,
)
from lamella.core.jobs.runner import JobRunner

__all__ = [
    "Job",
    "JobCancelled",
    "JobContext",
    "JobEvent",
    "JobOutcome",
    "JobRunner",
    "JobStatus",
    "TERMINAL_STATUSES",
]
