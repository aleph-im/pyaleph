pragma solidity ^0.5.7;

contract AlephSync{
    event SyncEvent(address addr, string message);

    function doEmit(string memory message) public {
        emit SyncEvent(msg.sender, message);
    }
}
