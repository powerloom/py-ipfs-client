import json
from urllib.parse import urljoin

from httpx import AsyncClient
from httpx import AsyncHTTPTransport
from httpx import Limits
from httpx import Timeout

import ipfs_client.exceptions
import ipfs_client.utils.addr as addr_util
from ipfs_client.dag import DAGSection
from ipfs_client.dag import IPFSAsyncClientError
from ipfs_client.default_logger import logger
from ipfs_client.settings.data_models import IPFSConfig
from ipfs_client.utils.s3 import S3Uploader


class AsyncIPFSClient:
    """
    Asynchronous client for interacting with IPFS nodes.

    This class provides methods for adding content to IPFS, retrieving content,
    and working with IPFS DAG (Directed Acyclic Graph) structures. It supports
    both read and write operations, with optional remote pinning and S3 integration.

    Attributes:
        _settings (IPFSConfig): Configuration settings for the IPFS client
        _client (AsyncClient): Asynchronous HTTP client for API requests
        _base_url (str): Base URL for IPFS API requests
        _host_numeric (int): Numeric representation of the host protocol
        dag (DAGSection): Interface for IPFS DAG operations
        _write_mode (bool): Whether this client instance has write capabilities
    """
    _settings: IPFSConfig
    _client: AsyncClient

    def __init__(
            self,
            addr,
            settings: IPFSConfig,
            api_base='api/v0',
            write_mode=False,

    ):
        """
        Initialize an AsyncIPFSClient instance.

        Args:
            addr (str): IPFS node address (multiaddr or URL)
            settings (IPFSConfig): Configuration settings for the client
            api_base (str): Base path for the IPFS API
            write_mode (bool): Whether this client instance has write capabilities

        Raises:
            ValueError: If the provided IPFS address is invalid
        """
        try:
            # Try to parse the address as a multiaddr
            self._base_url, \
                self._host_numeric = addr_util.multiaddr_to_url_data(
                    addr, api_base,
                )
        except ipfs_client.exceptions.AddressError:
            # Fall back to treating the address as a URL
            if not addr_util.is_valid_url(addr):
                raise ValueError('Invalid IPFS address')
            self._base_url = urljoin(addr, api_base)
            self._host_numeric = addr_util.P_TCP
        self.dag = None
        self._logger = logger.bind(module='IPFSAsyncClient')
        self._settings = settings
        self._write_mode = write_mode

        # Initialize S3 uploader if enabled
        if settings.s3.enabled:
            self._s3_uploader = S3Uploader(settings.s3)
        else:
            self._s3_uploader = None

    async def init_session(self):
        """
        Initialize the HTTP session for IPFS API communication.

        This method sets up the HTTP client with appropriate connection limits,
        timeouts, and authentication. If remote pinning is enabled and this is
        a write client, it also configures the remote pinning service.

        Raises:
            ValueError: If remote pinning is enabled but required settings are missing
            IPFSAsyncClientError: If remote pinning service configuration fails
        """
        # Configure connection limits based on settings
        conn_limits = self._settings.connection_limits
        self._async_transport = AsyncHTTPTransport(
            limits=Limits(
                max_connections=conn_limits.max_connections,
                max_keepalive_connections=conn_limits.max_connections,
                keepalive_expiry=conn_limits.keepalive_expiry,
            ),
        )
        # Prepare client initialization arguments
        client_init_args = dict(
            base_url=self._base_url,
            timeout=Timeout(self._settings.timeout),
            follow_redirects=False,
            transport=self._async_transport,
        )
        # Add authentication if configured
        if self._settings.url_auth:
            client_init_args.update(
                {
                    'auth': (
                        self._settings.url_auth.apiKey,
                        self._settings.url_auth.apiSecret,
                    ),
                },
            )
        self._client = AsyncClient(**client_init_args)

        # Configure remote pinning if enabled and in write mode
        if self._settings.remote_pinning.enabled and self._write_mode:
            # Validate that all required remote pinning settings are provided
            if not all(
                    [
                        self._settings.remote_pinning.service_name,
                        self._settings.remote_pinning.service_endpoint,
                        self._settings.remote_pinning.service_token,
                    ],
            ):
                raise ValueError(
                    'Remote pinning enabled but service_name, service_endpoint, or service_token not set',
                )

            # Register the remote pinning service with the IPFS node
            # curl -X POST "http://127.0.0.1:5001/api/v0/pin/remote/service/add?arg=<service>&arg=<endpoint>&arg=<key>"
            r = await self._client.post(
                url=f'/pin/remote/service/add?arg={self._settings.remote_pinning.service_name}&arg={self._settings.remote_pinning.service_endpoint}&arg={self._settings.remote_pinning.service_token}',
            )
            if r.status_code != 200:
                if r.status_code == 500:
                    # Handle case where service is already registered
                    try:
                        resp = json.loads(r.text)
                    except json.JSONDecodeError:
                        raise IPFSAsyncClientError(
                            f'IPFS client error: remote pinning service add operation, response:{r}',
                        )
                    else:
                        if resp['Message'] == 'service already present':
                            self._logger.debug(
                                'Remote pinning service already present',
                            )
                            pass
                        else:
                            raise IPFSAsyncClientError(
                                f'IPFS client error: remote pinning service add operation, response:{r}',
                            )
                else:
                    raise IPFSAsyncClientError(
                        f'IPFS client error: remote pinning service add operation, response:{r}',
                    )
            else:
                self._logger.debug(
                    'Remote pinning service added successfully',
                )

        # Initialize DAG operations interface
        self.dag = DAGSection(self._client)
        self._logger.debug('Inited IPFS client on base url {}', self._base_url)

    def add_str(self, string, **kwargs):
        """
        Add a string to IPFS.

        Args:
            string (str): The string to add to IPFS
            **kwargs: Additional arguments to pass to the IPFS API

        Returns:
            str: The CID of the added content

        Note:
            This method is not implemented yet.
        """
        # TODO
        pass

    async def add_bytes(self, data: bytes, **kwargs):
        """
        Add binary data to IPFS.

        This method uploads binary data to IPFS and returns the resulting CID.
        If S3 integration is enabled, it also uploads the content to S3.
        If remote pinning is enabled, it pins the content to the configured
        remote pinning service.

        Args:
            data (bytes): The binary data to add to IPFS
            **kwargs: Additional arguments to pass to the IPFS API

        Returns:
            str: The CID of the added content

        Raises:
            IPFSAsyncClientError: If the IPFS add operation fails
        """
        # Prepare the file data for upload
        files = {'': data}
        # Upload to IPFS with CID version 1
        r = await self._client.post(
            url='/add?cid-version=1',
            files=files,
        )
        if r.status_code != 200:
            raise IPFSAsyncClientError(
                f'IPFS client error: add_bytes operation, response:{r}',
            )

        # Parse the response to extract the CID
        try:
            resp = json.loads(r.text)
        except json.JSONDecodeError:
            return r.text
        else:
            generated_cid = resp['Hash']

        # Upload to S3 if enabled
        if self._settings.s3.enabled:
            await self._s3_uploader.upload_file(data=data, file_name=generated_cid)

        # Pin to remote pinning service if enabled
        if self._settings.remote_pinning.enabled:
            # curl -X POST "http://127.0.0.1:5001/api/v0/pin/remote/add?arg=<ipfs-path>&service=<value>&name=<value>&background=false"
            r = await self._client.post(
                url=f'/pin/remote/add?arg={generated_cid}&service={self._settings.remote_pinning.service_name}&background={self._settings.remote_pinning.background_pinning}',
            )
            if r.status_code != 200:
                self._logger.error(
                    f'IPFS client error: remote pinning add operation, response:{r}',
                )
        return generated_cid

    async def add_json(self, json_obj, **kwargs):
        """
        Add a JSON object to IPFS.

        This method serializes a Python object to JSON, uploads it to IPFS,
        and returns the resulting CID.

        Args:
            json_obj: The Python object to serialize and add to IPFS
            **kwargs: Additional arguments to pass to the IPFS API

        Returns:
            str: The CID of the added JSON content

        Raises:
            Exception: If JSON serialization fails
        """
        try:
            # Serialize the JSON object to UTF-8 encoded bytes
            json_data = json.dumps(json_obj).encode('utf-8')
        except Exception as e:
            raise e

        # Upload the serialized data using add_bytes
        cid = await self.add_bytes(json_data, **kwargs)
        return cid

    async def cat(self, cid, **kwargs):
        """
        Retrieve content from IPFS by its CID.

        This method fetches content from IPFS and returns it either as a string
        or as binary data, depending on the bytes_mode parameter.

        Args:
            cid (str): The CID of the content to retrieve
            **kwargs: Additional arguments, including:
                bytes_mode (bool): Whether to return binary data (True) or text (False)

        Returns:
            Union[str, bytes]: The retrieved content

        Raises:
            IPFSAsyncClientError: If the IPFS cat operation fails or returns empty content
        """
        # Determine whether to return binary data or text
        bytes_mode = kwargs.get('bytes_mode', False)
        if not bytes_mode:
            response_body = ''
        else:
            response_body = b''

        last_response_code = None
        # Stream the response to handle potentially large content
        async with self._client.stream(method='POST', url=f'/cat?arg={cid}') as response:
            if response.status_code != 200:
                raise IPFSAsyncClientError(
                    f'IPFS client error: cat on CID {cid}, response status code error: {response.status_code}',
                )
            # Accumulate the response chunks
            if not bytes_mode:
                async for chunk in response.aiter_text():
                    response_body += chunk
            else:
                async for chunk in response.aiter_bytes():
                    response_body += chunk
            last_response_code = response.status_code

        # Ensure we received some content
        if not response_body:
            raise IPFSAsyncClientError(
                f'IPFS client error: cat on CID {cid}, response body empty. response status code error: {last_response_code}',
            )
        return response_body

    async def get_json(self, cid, **kwargs):
        """
        Retrieve and parse JSON content from IPFS by its CID.

        This method fetches content from IPFS and attempts to parse it as JSON.
        If parsing fails, it returns the raw content as a string.

        Args:
            cid (str): The CID of the JSON content to retrieve
            **kwargs: Additional arguments to pass to the cat method

        Returns:
            Union[dict, list, str]: The parsed JSON object or raw content if parsing fails
        """
        # Retrieve the content as text
        json_data = await self.cat(cid)
        try:
            # Attempt to parse as JSON
            return json.loads(json_data)
        except json.JSONDecodeError:
            # Return raw content if parsing fails
            return json_data

    async def remove_bytes(self, cid, skip_s3_removal=False, skip_remote_pinning_removal=False, **kwargs):
        """
        Remove content from IPFS by its CID.

        This method unpins content from the local IPFS node. If remote pinning
        is enabled, it also removes the content from the remote pinning service.
        If S3 integration is enabled, it attempts to delete the content from S3.

        Args:
            cid (str): The CID of the content to remove
            **kwargs: Additional arguments to pass to the IPFS API

        Returns:
            bool: True if the removal was successful, False otherwise

        Raises:
            IPFSAsyncClientError: If the IPFS removal operation fails
        """
        # Ensure we're in write mode
        if not self._write_mode:
            raise IPFSAsyncClientError(
                'Cannot remove content in read-only mode',
            )

        # First, unpin from local node
        r = await self._client.post(url=f'/pin/rm?arg={cid}')
        if r.status_code != 200:
            self._logger.error(
                f'IPFS client error: local pin removal operation for CID {cid}, response:{r}',
            )
            return False

        # Remove from remote pinning service if enabled
        if self._settings.remote_pinning.enabled and not skip_remote_pinning_removal:
            r = await self._client.post(
                url=f'/pin/remote/rm?arg={cid}&service={self._settings.remote_pinning.service_name}',
            )
            if r.status_code != 200:
                self._logger.error(
                    f'IPFS client error: remote pin removal operation for CID {cid}, response:{r}',
                )

        # Delete from S3 if enabled
        if self._settings.s3.enabled and self._s3_uploader and not skip_s3_removal:
            try:
                await self._s3_uploader.delete_file(file_name=cid)
            except Exception as e:
                self._logger.error(
                    f'S3 deletion error for CID {cid}: {str(e)}',
                )

        return True

    async def remove_json(self, cid, **kwargs):
        """
        Remove JSON content from IPFS by its CID.

        This is a convenience wrapper around remove_bytes specifically for JSON content.

        Args:
            cid (str): The CID of the JSON content to remove
            **kwargs: Additional arguments to pass to the remove_bytes method

        Returns:
            bool: True if the removal was successful, False otherwise

        Raises:
            IPFSAsyncClientError: If the IPFS removal operation fails
        """
        return await self.remove_bytes(cid, **kwargs)


class AsyncIPFSClientSingleton:
    """
    Singleton wrapper for IPFS client instances.

    This class manages separate client instances for read and write operations,
    ensuring efficient resource usage and appropriate configuration for each
    operation type.

    Attributes:
        _ipfs_write_client (AsyncIPFSClient): Client instance for write operations
        _ipfs_read_client (AsyncIPFSClient): Client instance for read operations
        _initialized (bool): Whether the client sessions have been initialized
        _s3_uploader (Optional[S3Uploader]): S3 uploader instance if S3 integration is enabled
    """

    def __init__(self, settings: IPFSConfig):
        """
        Initialize the AsyncIPFSClientSingleton.

        Args:
            settings (IPFSConfig): Configuration settings for the IPFS clients
        """
        # Create separate clients for read and write operations
        self._ipfs_write_client = AsyncIPFSClient(
            addr=settings.url, settings=settings, write_mode=True,
        )
        self._ipfs_read_client = AsyncIPFSClient(
            addr=settings.reader_url, settings=settings, write_mode=False,
        )
        self._initialized = False

    async def init_sessions(self):
        """
        Initialize the HTTP sessions for both read and write clients.

        This method must be called before using the clients to ensure
        proper initialization of HTTP sessions and related resources.

        Returns:
            None
        """
        if self._initialized:
            return
        # Initialize both client sessions
        await self._ipfs_write_client.init_session()
        await self._ipfs_read_client.init_session()
        self._initialized = True
