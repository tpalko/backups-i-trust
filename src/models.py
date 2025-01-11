from frank.database.model import BaseModel
from frank.database.column import JsonColumn, StringColumn, IntColumn, FloatColumn, BoolColumn, DateTimeColumn, TextColumn

class Run(BaseModel):
    start_at = DateTimeColumn()
    end_at = DateTimeColumn()
    run_stats_json = JsonColumn()

class Archive(BaseModel):
    target_id = IntColumn()
    size_kb = IntColumn()
    is_remote = BoolColumn()
    remote_push_at = DateTimeColumn()
    filename = TextColumn()
    returncode = IntColumn()
    errors = TextColumn()
    pre_marker_timestamp = DateTimeColumn()
    md5 = StringColumn(size=32)
    uncompressed_size_kb = IntColumn()

class Target(BaseModel):
    path = TextColumn()
    name = StringColumn(size=64)
    excludes = TextColumn()
    budget_max = FloatColumn()
    frequency = StringColumn(size=32)
    push_strategy = StringColumn(size=32)
    push_period = IntColumn()
    is_active = BoolColumn()
    pre_marker_at = DateTimeColumn()
    post_marker_at = DateTimeColumn()
    last_reason = StringColumn(size=32)
