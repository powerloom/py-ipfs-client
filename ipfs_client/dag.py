import json
from io import BytesIO

from httpx import AsyncClient


class IPFSAsyncClientError(Exception):
    """
    Custom exception for IPFS client errors.
    
    This exception is raised when an IPFS API operation fails, providing
    details about the failure for debugging and error handling.
    """
    def __init__(self, message: str):
        """
        Initialize the exception with an error message.
        
        Args:
            message (str): Detailed error message describing the IPFS operation failure
        """
        super().__init__(message)
        self._message = message

    def __str__(self) -> str:
        """
        Return the string representation of the error.
        
        Returns:
            str: The error message
        """
        return self._message

    def __repr__(self) -> str:
        """
        Return the official string representation of the error.
        
        Returns:
            str: The error message
        """
        return self._message


class DAGBlock:
    """
    Represents a DAG (Directed Acyclic Graph) block from IPFS.
    
    This class provides methods to work with DAG blocks retrieved from IPFS,
    allowing access to the raw JSON or parsed Python objects.
    """
    def __init__(self, json_body: str):
        """
        Initialize a DAG block with JSON content.
        
        Args:
            json_body (str): JSON string representation of the DAG block
        """
        self._dag_block_json = json_body

    def as_json(self):
        """
        Parse and return the DAG block as a Python object.
        
        Returns:
            dict/list: The parsed JSON content as Python objects
            
        Raises:
            json.JSONDecodeError: If the JSON content cannot be parsed
        """
        return json.loads(self._dag_block_json)

    def __str__(self):
        """
        Return the string representation of the DAG block.
        
        Returns:
            str: The raw JSON string
        """
        return self._dag_block_json


class DAGSection:
    """
    Interface for IPFS DAG (Directed Acyclic Graph) operations.
    
    This class provides methods to interact with the IPFS DAG API,
    allowing for putting and retrieving content in the IPFS DAG.
    """
    def __init__(self, async_client: AsyncClient):
        """
        Initialize the DAG section with an async HTTP client.
        
        Args:
            async_client (AsyncClient): The HTTP client for making API requests
        """
        self._client: AsyncClient = async_client

    async def put(self, bytes_body: BytesIO, pin=True):
        """
        Add data to the IPFS DAG.
        
        This method uploads data to IPFS and stores it as a DAG node.
        
        Args:
            bytes_body (BytesIO): The data to add to the DAG
            pin (bool, optional): Whether to pin the data. Defaults to True.
            
        Returns:
            dict/str: The response from the IPFS API, typically containing the CID
                     of the added content
                     
        Raises:
            IPFSAsyncClientError: If the DAG put operation fails
        """
        # Prepare the file data for upload
        files = {'': bytes_body}
        
        # Make the API request to add data to the DAG
        r = await self._client.post(
            url=f'/dag/put?pin={str(pin).lower()}',
            files=files,
        )
        
        # Check if the operation was successful
        if r.status_code != 200:
            raise IPFSAsyncClientError(
                f'IPFS client error: dag-put operation, response:{r}',
            )
        
        # Parse and return the response
        try:
            return json.loads(r.text)
        except json.JSONDecodeError:
            return r.text

    async def get(self, dag_cid):
        """
        Retrieve data from the IPFS DAG.
        
        This method fetches a DAG node from IPFS by its CID (Content Identifier).
        
        Args:
            dag_cid (str): The CID of the DAG node to retrieve
            
        Returns:
            DAGBlock: An object representing the retrieved DAG node
            
        Raises:
            IPFSAsyncClientError: If the DAG get operation fails
        """
        # Make the API request to get data from the DAG
        response = await self._client.post(url=f'/dag/get?arg={dag_cid}')
        
        # Check if the operation was successful
        if response.status_code != 200:
            raise IPFSAsyncClientError(
                f'IPFS client error: dag-get operation, response:{response}',
            )

        # Return the DAG block
        return DAGBlock(response.text)
