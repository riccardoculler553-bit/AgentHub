"""Registration code creation API."""

from fastapi import APIRouter, Depends, status
from sqlalchemy.orm import Session

from app.auth.admin import require_admin
from app.db.database import get_db
from app.db.models import User
from app.device.models import CreateRegistrationCodeIn, RegistrationCodeOut
from app.registration.service import RegistrationService

router = APIRouter(prefix="/api", tags=["registration"], dependencies=[Depends(require_admin)])


def get_or_create_default_user(db: Session) -> User:
    """V1.0 has no user login yet; all codes belong to the default admin user."""
    from app.core.config import settings

    user = db.query(User).filter_by(username=settings.default_username).one_or_none()
    if user is None:
        user = User(username=settings.default_username)
        db.add(user)
        db.flush()
    return user


@router.post("/device-registration", response_model=RegistrationCodeOut, status_code=status.HTTP_201_CREATED)
def create_registration_code(payload: CreateRegistrationCodeIn, db: Session = Depends(get_db)):
    user = get_or_create_default_user(db)
    service = RegistrationService(db)
    row, plaintext = service.create_code(user_id=user.id)
    db.commit()
    return RegistrationCodeOut(registration_id=row.registration_id, code=plaintext, expires_at=row.expires_at)
