"""CapabilityService: definition + version lifecycle (V1.4 §8/§9/§57).

Server owns "有什么能力/能力是什么/版本"; Workers execute. Versions are
IMMUTABLE: creating an existing (name, version) fails instead of updating,
so every execution is reproducible/auditable (V1.4 §56).
"""

import re

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.capability_runtime.db_models import AutomationCapability, CapabilityVersion
from app.capability_runtime.errors import (
    CapabilityAlreadyExists,
    CapabilityDisabled,
    CapabilityInvalidName,
    CapabilityNotFound,
    CapabilityVersionExists,
    CapabilityVersionNotFound,
    CapabilityVersionNotPublished,
)
from app.db.models import utcnow

CAPABILITY_NAME_RE = re.compile(r"^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*){2}$")
CAPABILITY_TYPES = {"ATOMIC", "BUSINESS"}
RUNTIME_TYPES = {"YINGDAO", "PYTHON", "HTTP", "LOCAL"}
RISK_LEVELS = {"READ", "WRITE", "ACTION"}
VERSION_STATUSES = {"DRAFT", "PUBLISHED", "DEPRECATED", "DISABLED"}


class CapabilityService:
    def __init__(self, db: Session) -> None:
        self.db = db

    # ----------------------------------------------------------- definitions

    def create_capability(
        self,
        name: str,
        runtime_type: str,
        *,
        display_name: str = "",
        description: str = "",
        type: str = "ATOMIC",
        input_schema: dict | None = None,
        output_schema: dict | None = None,
        risk_level: str = "READ",
        requires_confirmation: bool = False,
    ) -> AutomationCapability:
        name = name.strip()
        if not CAPABILITY_NAME_RE.match(name):
            raise CapabilityInvalidName(name)
        if runtime_type not in RUNTIME_TYPES:
            raise ValueError(f"runtime_type must be one of {sorted(RUNTIME_TYPES)}: {runtime_type}")
        if type not in CAPABILITY_TYPES:
            raise ValueError(f"type must be one of {sorted(CAPABILITY_TYPES)}: {type}")
        if risk_level not in RISK_LEVELS:
            raise ValueError(f"risk_level must be one of {sorted(RISK_LEVELS)}: {risk_level}")
        if self.get_capability(name) is not None:
            raise CapabilityAlreadyExists(name)
        row = AutomationCapability(
            name=name,
            display_name=display_name or name,
            description=description,
            type=type,
            runtime_type=runtime_type,
            enabled=True,
            input_schema=input_schema or {},
            output_schema=output_schema or {},
            risk_level=risk_level,
            requires_confirmation=requires_confirmation,
        )
        self.db.add(row)
        self.db.commit()
        return row

    def get_capability(self, name: str) -> AutomationCapability | None:
        return self.db.scalars(
            select(AutomationCapability).where(AutomationCapability.name == name)
        ).first()

    def require_capability(self, name: str) -> AutomationCapability:
        row = self.get_capability(name)
        if row is None:
            raise CapabilityNotFound(name)
        return row

    def list_capabilities(self, enabled_only: bool = False) -> list[AutomationCapability]:
        stmt = select(AutomationCapability).order_by(AutomationCapability.name)
        if enabled_only:
            stmt = stmt.where(AutomationCapability.enabled.is_(True))
        return list(self.db.scalars(stmt))

    def update_capability(self, name: str, *, enabled: bool | None = None,
                          display_name: str | None = None,
                          description: str | None = None) -> AutomationCapability:
        row = self.require_capability(name)
        if enabled is not None:
            row.enabled = enabled
        if display_name is not None:
            row.display_name = display_name
        if description is not None:
            row.description = description
        row.updated_at = utcnow()
        self.db.commit()
        return row

    # -------------------------------------------------------------- versions

    def create_version(
        self,
        name: str,
        version: str,
        package_id: str,
        *,
        entrypoint: str = "main",
        input_schema: dict | None = None,
        output_schema: dict | None = None,
        checksum: str = "",
        config: dict | None = None,
    ) -> CapabilityVersion:
        """Create an immutable DRAFT version. Rejects duplicates (§9)."""
        self.require_capability(name)
        existing = self.get_version(name, version)
        if existing is not None:
            raise CapabilityVersionExists(name, version)
        row = CapabilityVersion(
            capability_name=name,
            version=version,
            package_id=package_id,
            status="DRAFT",
            config=config or {},
            entrypoint=entrypoint,
            input_schema=input_schema or {},
            output_schema=output_schema or {},
            checksum=checksum,
        )
        self.db.add(row)
        self.db.commit()
        return row

    def get_version(self, name: str, version: str) -> CapabilityVersion | None:
        return self.db.scalars(
            select(CapabilityVersion).where(
                CapabilityVersion.capability_name == name,
                CapabilityVersion.version == version,
            )
        ).first()

    def get_version_by_id(self, version_id: int) -> CapabilityVersion | None:
        return self.db.scalars(
            select(CapabilityVersion).where(CapabilityVersion.id == version_id)
        ).first()

    def list_versions(self, name: str) -> list[CapabilityVersion]:
        return list(
            self.db.scalars(
                select(CapabilityVersion)
                .where(CapabilityVersion.capability_name == name)
                .order_by(CapabilityVersion.id.desc())
            )
        )

    def publish_version(self, name: str, version: str) -> CapabilityVersion:
        """DRAFT -> PUBLISHED and promote to the capability's current_version.
        Only PUBLISHED versions are executable (V1.4 §38)."""
        self.require_capability(name)
        row = self.get_version(name, version)
        if row is None:
            raise CapabilityVersionNotFound(name, version)
        if row.status == "PUBLISHED":
            return row  # idempotent publish
        if row.status != "DRAFT":
            raise CapabilityVersionNotPublished(name, version, row.status)
        row.status = "PUBLISHED"
        capability = self.get_capability(name)
        capability.current_version = version
        capability.updated_at = utcnow()
        self.db.commit()
        return row

    def get_published_version(self, name: str, version: str | None = None) -> CapabilityVersion:
        """Resolve an EXECUTABLE version: the requested one, else the current
        PUBLISHED one. Task Engine dispatch requires a PUBLISHED status."""
        capability = self.require_capability(name)
        if not capability.enabled:
            raise CapabilityDisabled(name)
        target = version or capability.current_version
        if not target:
            raise CapabilityVersionNotFound(name, version or "(current)")
        row = self.get_version(name, target)
        if row is None:
            raise CapabilityVersionNotFound(name, target)
        if row.status != "PUBLISHED":
            raise CapabilityVersionNotPublished(name, target, row.status)
        return row
