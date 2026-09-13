"""Foz - a payments stream whose destination stays correct when the data is late.

Kafka (KRaft) -> Spark Structured Streaming -> Delta, with a job-owned watermark
so that late records are routed to a table instead of being dropped.
"""
