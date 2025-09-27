from fastapi import Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse


class AppException(Exception):
    def __init__(
        self,
        details: str | None = None,
        status_code: int = status.HTTP_500_INTERNAL_SERVER_ERROR,
    ):
        self.__details = "Unknown error" if details is None else details
        self.__status_code = status_code

    @property
    def details(self):
        return self.__details

    @property
    def status_code(self):
        return self.__status_code

    def __str__(self) -> str:
        return self.__details


class EntityNotFoundException(AppException):
    def __init__(self, details: str | None = None):
        super().__init__(
            ("Required entity was not found" if details is None else details),
            status.HTTP_404_NOT_FOUND,
        )


class EntityAlreadyExistsException(AppException):
    def __init__(self, details: str | None = None):
        super().__init__(
            ("Given entity already exists" if details is None else details),
            status.HTTP_409_CONFLICT,
        )


class UnauthorizedException(AppException):
    def __init__(self, details: str | None = None):
        super().__init__(
            "Unauthorized" if details is None else details,
            status.HTTP_401_UNAUTHORIZED,
        )


class ForbiddenException(AppException):
    def __init__(self, details: str | None = None):
        super().__init__(
            "Forbidden" if details is None else details,
            status.HTTP_403_FORBIDDEN,
        )


class BadRequestException(AppException):
    def __init__(self, details: str | None = None):
        super().__init__(
            "Bad Request" if details is None else details,
            status.HTTP_400_BAD_REQUEST,
        )


class InternalServerError(AppException):
    def __init__(self, details: str | None = None):
        super().__init__(
            ("Internal Server Error" if details is None else details),
            status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


def handle_app_exception(_: Request, exception: AppException):
    return JSONResponse(
        status_code=exception.status_code,
        content={"details": exception.details},
    )


def handle_validation_exception(_: Request, exception: RequestValidationError):
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content=jsonable_encoder({"details": exception.errors()}),
    )
