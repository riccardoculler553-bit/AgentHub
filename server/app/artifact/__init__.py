"""Artifact domain (V1.4 §28-§31).

An Artifact is any execution product (file/dataset/report/log) referenced by
artifact_id instead of a worker-local path. Metadata lives in the artifacts
table; the bytes live under storage/artifacts/YYYY/MM/ (swappable for
OSS/MinIO later).
"""
