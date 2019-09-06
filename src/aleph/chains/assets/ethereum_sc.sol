pragma solidity ^0.5.11;

contract AlephSync{

    event SyncEvent(uint256 timestamp, address addr, string message); 
    
    function doEmit(string memory message) public {
        emit SyncEvent(block.timestamp, msg.sender, message);
    }

}
