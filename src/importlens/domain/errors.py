class ImportLensError(Exception):
    code = "import_error"


class PreviewNotFound(ImportLensError):
    code = "preview_not_found"


class StaleRevision(ImportLensError):
    code = "stale_revision"


class PreviewBlocked(ImportLensError):
    code = "preview_blocked"


class NothingToApply(ImportLensError):
    code = "nothing_to_apply"


class RollbackUnavailable(ImportLensError):
    code = "rollback_unavailable"


class UploadTooLarge(ImportLensError):
    code = "upload_too_large"


class ImportCapacityReached(ImportLensError):
    code = "import_capacity_reached"
