import os

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

from app.auth import create_access_token, verify_token

router = APIRouter()


class LoginRequest(BaseModel):
    username: str
    password: str


@router.post("/login")
async def login(payload: LoginRequest):
    expected_user = os.getenv("APP_USERNAME", "trader")
    expected_password = os.getenv("APP_PASSWORD", "demo")
    if payload.username != expected_user or payload.password != expected_password:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid username or password",
        )
    token = create_access_token({"sub": payload.username})
    return {"access_token": token, "token_type": "bearer"}


@router.get("/me")
async def me(username: str = Depends(verify_token)):
    return {"username": username}
