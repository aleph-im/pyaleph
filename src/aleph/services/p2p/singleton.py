from typing import List, Optional

# TODO: this global variable is currently used to distribute the list of HTTP nodes
#       on the network. Rewrite the retry and storage modules to pass this state
#       as a parameter instead.
api_servers: Optional[List[str]] = None
