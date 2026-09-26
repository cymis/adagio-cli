"""Backend-agnostic task execution.

``coordinator`` decides when work items run, ``backends`` decide where they
run, and ``resources`` decides what each item requests. None of these modules
know about pipelines or QIIME: the whole-action layer in ``adagio.executors``
turns a pipeline into work items and reports their progress.
"""
