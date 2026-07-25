from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    target = Path(path)
    text = target.read_text(encoding="utf-8")
    count = text.count(old)
    if count != 1:
        raise SystemExit(
            f"expected one replacement in {path}, got {count}: {old[:140]!r}"
        )
    target.write_text(text.replace(old, new, 1), encoding="utf-8")


replace_once(
    "src/ibmd/catalog/sessions.py",
    '''    exceptions: tuple[SessionExceptionV1, ...]
    production_qualified: bool
    exception_coverage_end_date: str | None
    qualification_note: str
''',
    '''    exceptions: tuple[SessionExceptionV1, ...]
    production_qualified: bool
    exception_coverage_start_date: str | None
    exception_coverage_end_date: str | None
    qualification_note: str
''',
)

replace_once(
    "src/ibmd/catalog/sessions.py",
    '''        "exceptions",
        "production_qualified",
        "exception_coverage_end_date",
        "qualification_note",
''',
    '''        "exceptions",
        "production_qualified",
        "exception_coverage_start_date",
        "exception_coverage_end_date",
        "qualification_note",
''',
)

replace_once(
    "src/ibmd/catalog/sessions.py",
    '''        if self.exception_coverage_end_date is not None:
            parse_date(
                self.exception_coverage_end_date,
                field_name="exception_coverage_end_date",
            )
        object.__setattr__(
            self,
            "production_qualified",
            boolean(
                self.production_qualified,
                field_name="production_qualified",
            ),
        )
        if self.production_qualified and self.exception_coverage_end_date is None:
            raise CatalogError(
                "production-qualified session requires exception_coverage_end_date"
            )
''',
    '''        coverage_start = (
            None
            if self.exception_coverage_start_date is None
            else parse_date(
                self.exception_coverage_start_date,
                field_name="exception_coverage_start_date",
            )
        )
        coverage_end = (
            None
            if self.exception_coverage_end_date is None
            else parse_date(
                self.exception_coverage_end_date,
                field_name="exception_coverage_end_date",
            )
        )
        if (coverage_start is None) != (coverage_end is None):
            raise CatalogError(
                "session qualification coverage requires both start and end dates"
            )
        if (
            coverage_start is not None
            and coverage_end is not None
            and coverage_end < coverage_start
        ):
            raise CatalogError(
                "session qualification coverage end precedes start"
            )
        object.__setattr__(
            self,
            "exception_coverage_start_date",
            None if coverage_start is None else coverage_start.isoformat(),
        )
        object.__setattr__(
            self,
            "exception_coverage_end_date",
            None if coverage_end is None else coverage_end.isoformat(),
        )
        object.__setattr__(
            self,
            "production_qualified",
            boolean(
                self.production_qualified,
                field_name="production_qualified",
            ),
        )
        if self.production_qualified and coverage_start is None:
            raise CatalogError(
                "production-qualified session requires bounded exception coverage"
            )
''',
)

replace_once(
    "src/ibmd/catalog/sessions.py",
    '''    @property
    def zone(self) -> ZoneInfo:
        return ZoneInfo(self.timezone)
''',
    '''    def is_production_qualified_for(self, local_date: object) -> bool:
        if not self.production_qualified:
            return False
        observed = parse_date(local_date, field_name="local_date")
        start = parse_date(
            self.exception_coverage_start_date,
            field_name="exception_coverage_start_date",
        )
        end = parse_date(
            self.exception_coverage_end_date,
            field_name="exception_coverage_end_date",
        )
        return start <= observed <= end

    @property
    def zone(self) -> ZoneInfo:
        return ZoneInfo(self.timezone)
''',
)

replace_once(
    "src/ibmd/catalog/sessions.py",
    '''        coverage = value["exception_coverage_end_date"]
        return cls(
''',
    '''        coverage_start = value["exception_coverage_start_date"]
        coverage_end = value["exception_coverage_end_date"]
        return cls(
''',
)

replace_once(
    "src/ibmd/catalog/sessions.py",
    '''            production_qualified=value["production_qualified"],
            exception_coverage_end_date=(
                None if coverage is None else str(coverage)
            ),
            qualification_note=str(value["qualification_note"]),
''',
    '''            production_qualified=value["production_qualified"],
            exception_coverage_start_date=(
                None if coverage_start is None else str(coverage_start)
            ),
            exception_coverage_end_date=(
                None if coverage_end is None else str(coverage_end)
            ),
            qualification_note=str(value["qualification_note"]),
''',
)

replace_once(
    "src/ibmd/catalog/sessions.py",
    '''            "exceptions": [item.to_dict() for item in self.exceptions],
            "production_qualified": self.production_qualified,
            "exception_coverage_end_date": self.exception_coverage_end_date,
            "qualification_note": self.qualification_note,
''',
    '''            "exceptions": [item.to_dict() for item in self.exceptions],
            "production_qualified": self.production_qualified,
            "exception_coverage_start_date": (
                self.exception_coverage_start_date
            ),
            "exception_coverage_end_date": self.exception_coverage_end_date,
            "qualification_note": self.qualification_note,
''',
)

replace_once(
    "src/ibmd/catalog/resolver.py",
    '''    local_date = local.date().isoformat()
    local_time = local.time().replace(microsecond=0).isoformat()
''',
    '''    local_date = local.date().isoformat()
    local_time = local.time().replace(microsecond=0).isoformat()
    production_qualified = session.is_production_qualified_for(local_date)
''',
)

text_path = Path("src/ibmd/catalog/resolver.py")
text = text_path.read_text(encoding="utf-8")
old = "production_qualified=session.production_qualified,"
if text.count(old) != 2:
    raise SystemExit("expected two static session qualification assignments")
text_path.write_text(
    text.replace(old, "production_qualified=production_qualified,"),
    encoding="utf-8",
)

replace_once(
    "src/ibmd/catalog/resolver.py",
    '''def require_production_qualified_session(session: SessionDefinitionV1) -> None:
    if not session.production_qualified:
        raise CatalogError(
            "session calendar is not production-qualified: "
            f"session={session.session_id}, note={session.qualification_note}"
        )
''',
    '''def require_production_qualified_session(
    session: SessionDefinitionV1,
    *,
    at_utc: datetime | str | None = None,
) -> None:
    if at_utc is None:
        qualified = session.production_qualified
        local_date = None
    else:
        observed = (
            parse_utc(at_utc)
            if isinstance(at_utc, str)
            else ensure_utc(at_utc)
        )
        local_date = observed.astimezone(session.zone).date().isoformat()
        qualified = session.is_production_qualified_for(local_date)
    if not qualified:
        raise CatalogError(
            "session calendar is not production-qualified for the requested "
            "date: "
            f"session={session.session_id}, local_date={local_date}, "
            f"coverage={session.exception_coverage_start_date}.."
            f"{session.exception_coverage_end_date}, "
            f"note={session.qualification_note}"
        )
''',
)

replace_once(
    "src/ibmd/execution/application/liquidation_triggers.py",
    '''    if require_production_session and not session.production_qualified:
''',
    '''    if (
        require_production_session
        and not session.is_production_qualified_for(due_local.date())
    ):
''',
)

replace_once(
    "tester/target_catalog_tester.py",
    '''        self.assertFalse(self.session.production_qualified)
        self.assertIsNone(self.session.exception_coverage_end_date)
''',
    '''        self.assertFalse(self.session.production_qualified)
        self.assertIsNone(self.session.exception_coverage_start_date)
        self.assertIsNone(self.session.exception_coverage_end_date)
''',
)
