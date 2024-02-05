import json

import aiohttp
import pytest

from aiohttp import web
from aiohttp.test_utils import make_mocked_coro

from aleph.web.controllers.storage import MAX_UNAUTHENTICATED_UPLOAD_FILE_SIZE

"""
async def storage_add_file(request: web.Request):
    storage_service = get_storage_service_from_request(request)
    session_factory = get_session_factory_from_request(request)
    signature_verifier = get_signature_verifier_from_request(request)
    config = get_config_from_request(request)
    grace_period = config.storage.grace_period.value

    post = await request.post()
    headers = request.headers
    try:
        file_field = post["file"]
    except KeyError:
        raise web.HTTPUnprocessableEntity(reason="Missing 'file' in multipart form.")

    if isinstance(file_field, FileField):
        content_length = int(headers.get("Content-Length")) if headers.get("Content-Length") else None
        uploaded_file: UploadedFile = MultipartUploadedFile(file_field, content_length)
    else:
        uploaded_file = RawUploadedFile(file_field)

    metadata = post.get("metadata")

    status_code = 200

    if metadata:
        metadata_bytes = (
            metadata.file.read() if isinstance(metadata, FileField) else metadata
        )
        try:
            storage_metadata = StorageMetadata.parse_raw(metadata_bytes)
        except ValidationError as e:
            raise web.HTTPUnprocessableEntity(
                reason=f"Could not decode metadata: {e.json()}"
            )

        message = storage_metadata.message
        sync = storage_metadata.sync
        max_upload_size = MAX_UPLOAD_FILE_SIZE

    else:
        # User did not provide a message in the `metadata` field
        message = None
        sync = False
        max_upload_size = MAX_UNAUTHENTICATED_UPLOAD_FILE_SIZE

    if uploaded_file.size > max_upload_size:
        raise web.HTTPRequestEntityTooLarge(
            actual_size=uploaded_file.size, max_size=MAX_UPLOAD_FILE_SIZE
        )

    with session_factory() as session:
        file_hash = await _check_and_add_file(
            session=session,
            signature_verifier=signature_verifier,
            storage_service=storage_service,
            message=message,
            file=uploaded_file,
            grace_period=grace_period,
        )
        session.commit()

    if message:
        broadcast_status = await broadcast_and_process_message(
            pending_message=message, sync=sync, request=request, logger=logger
        )
        status_code = broadcast_status_to_http_status(broadcast_status)

    output = {"status": "success", "hash": file_hash}
    return web.json_response(data=output, status=status_code)
"""
@pytest.mark.asyncio
async def test_storage_add_file_unauthenticated(mock_config, ccn_api_client, mocker):
    # Prepare the file and metadata for the POST request
    # Here, you simulate an oversized file for unauthenticated requests
    data = aiohttp.FormData()
    data.add_field('file', 'fake_file_content', filename='test.txt', content_type='text/plain')
    # Simulate file size exceeding limit by setting Content-Length header manually in the request
    headers = {'Content-Length': str(MAX_UNAUTHENTICATED_UPLOAD_FILE_SIZE + 1)}

    # Perform the POST request using the mocked ccn_api_client
    response = await ccn_api_client.post("/api/v0/storage/add_file", data=data, headers=headers)

    # Assert that the response status code indicates a request entity too large error
    assert response.status == web.HTTPRequestEntityTooLarge.status_code, "Expected HTTP 413 Request Entity Too Large"
