from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.db import get_db
from app.dependencies.auth import get_current_user_context
from app.models.auth import UserAccount
from app.models.auth_api import LoginRequest, RegisterRequest, TokenResponse
from app.services.auth import authenticate_user, create_access_token, create_user

router = APIRouter(prefix="/auth", tags=["auth"])


@router.post("/register", response_model=TokenResponse)
async def register(request: RegisterRequest, db: Session = Depends(get_db)) -> TokenResponse:
    existing = db.query(UserAccount).filter(UserAccount.email == request.email.lower()).one_or_none()
    if existing is not None:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Account already exists.")
    user = create_user(email=request.email, password=request.password, db=db)
    user_id = str(user.id)
    token = create_access_token(user_id=user_id, email=user.email)
    return TokenResponse(access_token=token, user_id=user_id, email=user.email)


@router.post("/login", response_model=TokenResponse)
async def login(request: LoginRequest, db: Session = Depends(get_db)) -> TokenResponse:
    user = authenticate_user(email=request.email, password=request.password, db=db)
    if user is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid email or password.")
    user_id = str(user.id)
    token = create_access_token(user_id=user_id, email=user.email)
    return TokenResponse(access_token=token, user_id=user_id, email=user.email)


@router.get("/me")
async def me(context: dict = Depends(get_current_user_context)) -> dict:
    return {"user_id": context["user_id"], "email": context["email"]}
